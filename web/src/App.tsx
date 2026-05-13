import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";
import { AppShell } from "@/components/layout/AppShell";
import { Dashboard } from "@/pages/Dashboard";
import { Sources } from "@/pages/Sources";
import { Scheduler } from "@/pages/Scheduler";
import { Queue } from "@/pages/Queue";
import { Pipeline } from "@/pages/Pipeline";
import { RawRecords } from "@/pages/RawRecords";
import { ParsedRecords } from "@/pages/ParsedRecords";
import { Normalization } from "@/pages/Normalization";
import { Dedup } from "@/pages/Dedup";
import { OfficeMatcher } from "@/pages/OfficeMatcher";
import { Notifications } from "@/pages/Notifications";
import { DLQ } from "@/pages/DLQ";
import { Metrics } from "@/pages/Metrics";
import { Logs } from "@/pages/Logs";
import { Settings } from "@/pages/Settings";
import { NotFound } from "@/pages/NotFound";

const OfficeMap = lazy(() => import("@/pages/OfficeMap").then((module) => ({ default: module.OfficeMap })));

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/sources" element={<Sources />} />
        <Route path="/scheduler" element={<Scheduler />} />
        <Route path="/queue" element={<Queue />} />
        <Route path="/pipeline" element={<Pipeline />} />
        <Route path="/raw" element={<RawRecords />} />
        <Route path="/parsed" element={<ParsedRecords />} />
        <Route path="/normalization" element={<Normalization />} />
        <Route path="/dedup" element={<Dedup />} />
        <Route path="/offices" element={<OfficeMatcher />} />
        <Route
          path="/map"
          element={
            <Suspense fallback={<div className="h-40 animate-pulse rounded-lg border border-line bg-bg-elevated/60" />}>
              <OfficeMap />
            </Suspense>
          }
        />
        <Route path="/notifications" element={<Notifications />} />
        <Route path="/dlq" element={<DLQ />} />
        <Route path="/metrics" element={<Metrics />} />
        <Route path="/logs" element={<Logs />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </AppShell>
  );
}
