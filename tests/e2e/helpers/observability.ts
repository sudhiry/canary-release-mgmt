// tests/e2e/helpers/observability.ts
import axios from "axios";

export interface PromInstantSample {
  metric: Record<string, string>;
  value: [number, string];
}

export interface PromInstantResponse {
  status: "success" | "error";
  data: { resultType: "vector"; result: PromInstantSample[] };
}

export async function queryPrometheus(localPort: number, query: string): Promise<PromInstantResponse> {
  const r = await axios.get(`http://localhost:${localPort}/api/v1/query`, {
    params: { query },
    timeout: 5000,
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`prometheus query failed (status=${r.status}, query=${query}): ${JSON.stringify(r.data)}`);
  }
  return r.data as PromInstantResponse;
}

export async function getGrafanaDashboard(localPort: number, uid: string): Promise<unknown> {
  const r = await axios.get(`http://localhost:${localPort}/api/dashboards/uid/${encodeURIComponent(uid)}`, {
    timeout: 5000,
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`grafana dashboard fetch failed (status=${r.status}, uid=${uid}): ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

export interface JaegerTraceSummary {
  traceID: string;
  spans: Array<{ operationName: string; tags?: Array<{ key: string; value: unknown }> }>;
}

export async function searchJaegerTraces(
  localPort: number,
  opts: { service: string; tags?: Record<string, string>; lookbackHours?: number; limit?: number },
): Promise<JaegerTraceSummary[]> {
  const params: Record<string, string> = {
    service: opts.service,
    limit: String(opts.limit ?? 20),
    lookback: `${opts.lookbackHours ?? 1}h`,
  };
  if (opts.tags && Object.keys(opts.tags).length > 0) {
    params.tags = JSON.stringify(opts.tags);
  }
  const r = await axios.get(`http://localhost:${localPort}/api/traces`, {
    params,
    timeout: 5000,
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`jaeger search failed (status=${r.status}, service=${opts.service}): ${JSON.stringify(r.data)}`);
  }
  const body = r.data as { data?: JaegerTraceSummary[] };
  return body.data ?? [];
}
