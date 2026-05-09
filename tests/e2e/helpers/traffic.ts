import axios from "axios";

export interface SendOrderOpts {
  url?: string;
  canary?: boolean;
  user?: string;
  sku?: string;
  quantity?: number;
  amount?: number;
}

export interface SendOrderResult {
  status: number;
  data: unknown;
  headers: Record<string, string>;
}

export async function sendOrder(opts: SendOrderOpts = {}): Promise<SendOrderResult> {
  const url = opts.url ?? "http://localhost:8080";
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.canary) headers["x-canary"] = "true";
  const r = await axios.post(
    `${url}/api/orders`,
    {
      userId: opts.user ?? "u1",
      sku: opts.sku ?? "sku-1",
      quantity: opts.quantity ?? 1,
      amount: opts.amount ?? 100,
    },
    { headers, validateStatus: () => true },
  );
  return { status: r.status, data: r.data, headers: r.headers as Record<string, string> };
}
