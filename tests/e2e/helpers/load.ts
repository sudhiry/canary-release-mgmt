import axios, { type AxiosRequestConfig } from "axios";

export interface LoadOpts {
  url: string;
  method?: "GET" | "POST";
  rps: number;
  durationSeconds: number;
  headers?: Record<string, string>;
  payload?: unknown;
}

export interface LoadStats {
  requestsSent: number;
  successCount: number;
  failureCount: number;
  p50Ms: number;
  p99Ms: number;
  totalDurationMs: number;
  errorSamples: string[];
}

export async function runLoad(opts: LoadOpts): Promise<LoadStats> {
  const intervalMs = 1000 / opts.rps;
  const start = Date.now();
  const deadline = start + opts.durationSeconds * 1000;
  const latencies: number[] = [];
  const errors: string[] = [];
  let sent = 0;
  let success = 0;
  let failure = 0;
  const inFlight: Promise<void>[] = [];

  while (Date.now() < deadline) {
    const reqStart = Date.now();
    sent++;
    const cfg: AxiosRequestConfig = {
      method: opts.method ?? "POST",
      url: opts.url,
      headers: opts.headers,
      data: opts.payload,
      validateStatus: () => true,
    };
    const p = axios(cfg).then(
      (r) => {
        const lat = Date.now() - reqStart;
        latencies.push(lat);
        if (r.status >= 200 && r.status < 300) success++;
        else {
          failure++;
          if (errors.length < 5) errors.push(`HTTP ${r.status}`);
        }
      },
      (err: Error) => {
        failure++;
        if (errors.length < 5) errors.push(err.message);
      },
    );
    inFlight.push(p);
    await new Promise<void>((res) => setTimeout(res, intervalMs));
  }

  await Promise.all(inFlight);
  latencies.sort((a, b) => a - b);
  const p50 = latencies.length === 0 ? 0 : latencies[Math.floor(latencies.length * 0.5)];
  const p99 = latencies.length === 0 ? 0 : latencies[Math.floor(latencies.length * 0.99)];

  return {
    requestsSent: sent,
    successCount: success,
    failureCount: failure,
    p50Ms: p50,
    p99Ms: p99,
    totalDurationMs: Date.now() - start,
    errorSamples: errors,
  };
}
