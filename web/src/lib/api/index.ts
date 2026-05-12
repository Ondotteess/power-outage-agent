import type { ApiClient } from "./client";
import { mockClient } from "./mock";
import { realClient } from "./real";

const useMock = import.meta.env.VITE_USE_MOCK !== "0";

export const api: ApiClient = useMock ? mockClient : realClient;
export const usingMock = useMock;
export * from "./types";
