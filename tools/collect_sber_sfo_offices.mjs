#!/usr/bin/env node

import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import path from "node:path";

const BANKI_CITY_API = "https://www.banki.ru/products/api/cities";
const BANKI_BRANCH_BASE = "https://www.banki.ru/banks/bank/sberbank/branches";
const DEFAULT_OUT_JSON = "data/offices/sber_sfo_offices.banki.json";
const DEFAULT_OUT_CSV = "data/offices/sber_sfo_offices.banki.csv";
const DEFAULT_META = "data/offices/sber_sfo_offices.banki.meta.json";
const DEFAULT_OUT_SQLITE = "data/offices/sber_sfo_offices.banki.sqlite";

const SFO_REGIONS = new Set([
  "Алтайский край",
  "Иркутская область",
  "Кемеровская область",
  "Красноярский край",
  "Новосибирская область",
  "Омская область",
  "Республика Алтай",
  "Республика Тыва",
  "Республика Хакасия",
  "Томская область",
]);

const DISCOVERY_QUERIES = [
  ..."абвгдеёжзийклмнопрстуфхцчшщэюя",
  "нов",
  "крас",
  "алтай",
  "кем",
  "том",
  "ом",
  "ирк",
  "абак",
  "кыз",
  "горно",
].filter(Boolean);

function parseArgs(argv) {
  const args = {
    outJson: DEFAULT_OUT_JSON,
    outCsv: DEFAULT_OUT_CSV,
    meta: DEFAULT_META,
    outSqlite: DEFAULT_OUT_SQLITE,
    fromJson: "",
    concurrency: 5,
    importUrl: "",
    token: process.env.OFFICE_IMPORT_TOKEN || "",
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--out-json") args.outJson = next, i += 1;
    else if (arg === "--out-csv") args.outCsv = next, i += 1;
    else if (arg === "--meta") args.meta = next, i += 1;
    else if (arg === "--out-sqlite") args.outSqlite = next, i += 1;
    else if (arg === "--from-json") args.fromJson = next, i += 1;
    else if (arg === "--concurrency") args.concurrency = Number(next), i += 1;
    else if (arg === "--import-url") args.importUrl = next, i += 1;
    else if (arg === "--token") args.token = next, i += 1;
    else if (arg === "--help") {
      console.log(`Usage:
  node tools/collect_sber_sfo_offices.mjs [options]

Options:
  --out-json PATH      JSON payload for /api/offices/import
  --out-csv PATH       Flat CSV for manual review
  --meta PATH          Collection metadata path
  --out-sqlite PATH    Standalone SQLite DB path
  --from-json PATH     Rebuild outputs from an existing JSON payload without refetching
  --concurrency N      City page fetch concurrency, default 5
  --import-url URL     Optional POST target, e.g. http://localhost:8000/api/offices/import
  --token TOKEN        Optional X-Import-Token for protected imports
`);
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  args.concurrency = Math.max(1, Math.min(12, Number.isFinite(args.concurrency) ? args.concurrency : 5));
  return args;
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: { "User-Agent": "PowerOutageAgent/0.1 office-import" },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${url}`);
  }
  return response.json();
}

async function fetchText(url) {
  const response = await fetch(url, {
    headers: { "User-Agent": "PowerOutageAgent/0.1 office-import" },
  });
  if (response.status === 404) return "";
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${url}`);
  }
  return response.text();
}

async function discoverSfoCities() {
  const cities = new Map();
  for (const query of DISCOVERY_QUERIES) {
    const url = `${BANKI_CITY_API}?name=${encodeURIComponent(query)}&limit=1000`;
    const payload = await fetchJson(url);
    for (const city of payload.data || []) {
      if (!SFO_REGIONS.has(city.area_name)) continue;
      cities.set(city.id, city);
    }
  }
  return [...cities.values()].sort((left, right) => {
    const region = left.area_name.localeCompare(right.area_name, "ru");
    return region || left.name.localeCompare(right.name, "ru");
  });
}

function decodeHtmlAttribute(value) {
  return value
    .replaceAll("&quot;", "\"")
    .replaceAll("&#x27;", "'")
    .replaceAll("&#x2F;", "/")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&amp;", "&");
}

function extractBranchOptions(html) {
  let pos = 0;
  const attrStart = "data-module-options=\"";
  while (true) {
    const startMarker = html.indexOf(attrStart, pos);
    if (startMarker < 0) return null;
    const start = startMarker + attrStart.length;
    const end = html.indexOf("\"", start);
    if (end < 0) return null;
    const raw = html.slice(start, end);
    if (raw.includes("&quot;resultsMap&quot;")) {
      return JSON.parse(decodeHtmlAttribute(raw));
    }
    pos = end + 1;
  }
}

function cityNameForImport(city) {
  return String(city.name || "").replace(/\s+\(.+\)$/, "").trim();
}

function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function toOfficeRows(city, options, collectedAt) {
  const mapRows = options?.resultsMap?.data || [];
  const listRows = options?.results?.data || [];
  const detailsById = new Map(listRows.map((row) => [row.id, row]));
  const sourceUrl = `${BANKI_BRANCH_BASE}/${city.region_url}/`;

  return mapRows
    .filter((row) => row.type === "office")
    .map((row) => {
      const details = detailsById.get(row.id) || {};
      const name = cleanText(row.name || details.name || "Офис Сбер");
      return {
        name: `Сбербанк — ${name}`,
        city: cityNameForImport(city),
        address: cleanText(row.address || details.address),
        region: city.area_name,
        is_active: row.active !== false,
        latitude: typeof row.latitude === "number" ? row.latitude : null,
        longitude: typeof row.longitude === "number" ? row.longitude : null,
        extra: {
          company: "ПАО Сбербанк",
          federal_district: "Сибирский федеральный округ",
          source: "banki.ru",
          source_url: sourceUrl,
          banki_city_id: city.id,
          banki_region_url: city.region_url,
          banki_area_name: city.area_name,
          banki_office_id: row.id,
          banki_region_id: row.region_id,
          office_type: row.type,
          bank_id: row.bank_id,
          bank_site: details.bank_site || "www.sberbank.ru",
          phone: details.phone || null,
          schedule_general: cleanText(details.schedule_general),
          schedule_private_person: cleanText(details.schedule_private_person),
          schedule_entities: cleanText(details.schedule_entities),
          schedule_vip: cleanText(details.schedule_vip),
          collected_at: collectedAt,
        },
      };
    })
    .filter((row) => row.name && row.city && row.address && row.region);
}

async function collectCity(city, index, total) {
  const url = `${BANKI_BRANCH_BASE}/${city.region_url}/`;
  const html = await fetchText(url);
  if (!html) {
    return { city, url, offices: [], error: "404" };
  }
  const options = extractBranchOptions(html);
  if (!options) {
    return { city, url, offices: [], error: "branch data not found" };
  }
  const offices = toOfficeRows(city, options, new Date().toISOString());
  if ((index + 1) % 25 === 0 || offices.length > 0) {
    console.error(`[${index + 1}/${total}] ${city.area_name} / ${city.name}: ${offices.length}`);
  }
  return {
    city,
    url,
    offices,
    reportedTotal: options.resultsMap?.total ?? null,
  };
}

async function mapConcurrent(items, concurrency, mapper) {
  const results = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const current = cursor;
      cursor += 1;
      try {
        results[current] = await mapper(items[current], current, items.length);
      } catch (error) {
        results[current] = { city: items[current], offices: [], error: String(error?.message || error) };
        console.error(`[${current + 1}/${items.length}] ${items[current].name}: ERROR ${error?.message || error}`);
      }
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return results;
}

function dedupeOffices(offices) {
  const byKey = new Map();
  for (const office of offices) {
    const key = [office.name, office.city, office.address].map((part) => part.toLocaleLowerCase("ru")).join("|");
    if (!byKey.has(key)) byKey.set(key, office);
  }
  return [...byKey.values()].sort((left, right) => {
    const region = left.region.localeCompare(right.region, "ru");
    const city = left.city.localeCompare(right.city, "ru");
    return region || city || left.name.localeCompare(right.name, "ru");
  });
}

function toCsv(rows) {
  const header = ["name", "city", "address", "region", "is_active", "latitude", "longitude", "source_url", "banki_office_id"];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      header
        .map((key) => {
          const value =
            key === "source_url" || key === "banki_office_id"
              ? row.extra[key]
              : row[key];
          return csvCell(value);
        })
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
}

async function writeSqlite(filePath, offices, meta) {
  await ensureParent(filePath);
  await rm(filePath, { force: true });
  await rm(`${filePath}-wal`, { force: true });
  await rm(`${filePath}-shm`, { force: true });
  const db = new DatabaseSync(filePath);
  db.exec(`
    PRAGMA journal_mode = DELETE;
    CREATE TABLE offices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      city TEXT NOT NULL,
      address TEXT NOT NULL,
      region TEXT NOT NULL,
      is_active INTEGER NOT NULL,
      latitude REAL,
      longitude REAL,
      extra TEXT NOT NULL,
      source_url TEXT,
      banki_office_id INTEGER,
      collected_at TEXT
    );
    CREATE UNIQUE INDEX uq_offices_name_city_address
      ON offices (name, city, address);

    CREATE TABLE import_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  const insert = db.prepare(`
    INSERT INTO offices (
      name, city, address, region, is_active, latitude, longitude,
      extra, source_url, banki_office_id, collected_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertMeta = db.prepare("INSERT INTO import_meta (key, value) VALUES (?, ?)");

  db.exec("BEGIN");
  try {
    for (const office of offices) {
      insert.run(
        office.name,
        office.city,
        office.address,
        office.region,
        office.is_active ? 1 : 0,
        office.latitude,
        office.longitude,
        JSON.stringify(office.extra),
        office.extra.source_url,
        office.extra.banki_office_id,
        office.extra.collected_at,
      );
    }
    for (const [key, value] of Object.entries(meta)) {
      insertMeta.run(key, typeof value === "string" ? value : JSON.stringify(value));
    }
    db.exec("COMMIT");
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // The original error is more useful than a rollback failure.
    }
    throw error;
  } finally {
    db.close();
  }
}

function csvCell(value) {
  if (value === null || value === undefined) return "";
  const text = String(value);
  if (!/[",\n\r]/.test(text)) return text;
  return `"${text.replaceAll("\"", "\"\"")}"`;
}

async function ensureParent(filePath) {
  await mkdir(path.dirname(filePath), { recursive: true });
}

async function maybeImport(importUrl, token, offices) {
  if (!importUrl) return null;
  const response = await fetch(importUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { "X-Import-Token": token } : {}),
    },
    body: JSON.stringify({ offices }),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Import failed: ${response.status} ${response.statusText}: ${text}`);
  }
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function countRegions(offices) {
  return offices.reduce((acc, office) => {
    acc[office.region] = (acc[office.region] || 0) + 1;
    return acc;
  }, {});
}

async function loadExistingMeta(metaPath) {
  try {
    return JSON.parse(await readFile(metaPath, "utf8"));
  } catch {
    return {};
  }
}

async function main() {
  const args = parseArgs(process.argv);
  let offices;
  let meta;

  if (args.fromJson) {
    console.error(`Loading offices from ${args.fromJson}...`);
    const payload = JSON.parse(await readFile(args.fromJson, "utf8"));
    const previousMeta = await loadExistingMeta(args.meta);
    offices = dedupeOffices(payload.offices || []);
    meta = {
      source: previousMeta.source || "banki.ru",
      bank: previousMeta.bank || "sberbank",
      federal_district: previousMeta.federal_district || "Сибирский федеральный округ",
      generated_at: new Date().toISOString(),
      candidate_cities: previousMeta.candidate_cities ?? null,
      cities_with_offices: previousMeta.cities_with_offices ?? null,
      offices: offices.length,
      regions: countRegions(offices),
      errors: previousMeta.errors || [],
    };
  } else {
    console.error("Discovering SFO cities on Banki.ru...");
    const cities = await discoverSfoCities();
    console.error(`Discovered ${cities.length} SFO city/locality candidates.`);

    const results = await mapConcurrent(cities, args.concurrency, collectCity);
    offices = dedupeOffices(results.flatMap((result) => result.offices || []));
    meta = {
      source: "banki.ru",
      bank: "sberbank",
      federal_district: "Сибирский федеральный округ",
      generated_at: new Date().toISOString(),
      candidate_cities: cities.length,
      cities_with_offices: results.filter((result) => (result.offices || []).length > 0).length,
      offices: offices.length,
      regions: countRegions(offices),
      errors: results
        .filter((result) => result.error && result.error !== "404")
        .map((result) => ({
          city: result.city?.name,
          region: result.city?.area_name,
          error: result.error,
        })),
    };
  }

  await ensureParent(args.outJson);
  await ensureParent(args.outCsv);
  await ensureParent(args.meta);
  await writeFile(args.outJson, `${JSON.stringify({ offices }, null, 2)}\n`, "utf8");
  await writeFile(args.outCsv, toCsv(offices), "utf8");
  await writeFile(args.meta, `${JSON.stringify(meta, null, 2)}\n`, "utf8");
  await writeSqlite(args.outSqlite, offices, meta);

  let importResult = null;
  if (args.importUrl) {
    importResult = await maybeImport(args.importUrl, args.token, offices);
  }

  console.log(
    JSON.stringify(
      {
        ...meta,
        out_json: args.outJson,
        out_csv: args.outCsv,
        out_sqlite: args.outSqlite,
        meta: args.meta,
        import_result: importResult,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
