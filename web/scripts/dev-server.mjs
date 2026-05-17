/* global process, setInterval */

import path from "node:path";
import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { createServer } from "vite";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const apiProxyTarget = process.env.VITE_API_PROXY_TARGET ?? "http://localhost:8000";

function readArg(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index >= 0 && process.argv[index + 1]) return process.argv[index + 1];
  const equals = process.argv.find((arg) => arg.startsWith(`${name}=`));
  return equals ? equals.slice(name.length + 1) : fallback;
}

const host = readArg("--host", process.env.VITE_HOST ?? "127.0.0.1");
const port = Number(readArg("--port", process.env.VITE_PORT ?? "5173"));

const server = await createServer({
  configFile: false,
  root,
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(root, "src"),
    },
  },
  server: {
    host,
    port,
    strictPort: true,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
});

await server.listen();
server.printUrls();
setInterval(() => {}, 2 ** 31 - 1);
