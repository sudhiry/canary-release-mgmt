import { Command } from "commander";
import axios from "axios";

export interface SendOrderOpts {
  url: string;
  canary: boolean;
  user: string;
  sku: string;
  quantity: number;
  amount: number;
}

export interface SendResult {
  status: number;
  data: unknown;
  headers: Record<string, string>;
}

export async function sendOrder(opts: SendOrderOpts): Promise<SendResult> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.canary) headers["x-canary"] = "true";
  const r = await axios.post(
    `${opts.url}/api/orders`,
    { userId: opts.user, sku: opts.sku, quantity: opts.quantity, amount: opts.amount },
    { headers, validateStatus: () => true },
  );
  return { status: r.status, data: r.data, headers: r.headers as Record<string, string> };
}

export async function run(argv: string[]): Promise<void> {
  const program = new Command();
  program
    .name("traffic-cli")
    .description("Send a request to the canary-release-mgmt edge with optional x-canary header");

  program.command("order")
    .description("POST /api/orders via the kind ingress")
    .option("--url <url>", "ingress base URL", "http://localhost:8080")
    .option("--canary", "send the x-canary: true header", false)
    .option("--user <id>", "userId", "u1")
    .option("--sku <sku>", "SKU", "sku-1")
    .option("--quantity <n>", "quantity", (v) => parseInt(v, 10), 1)
    .option("--amount <n>", "amount", (v) => parseInt(v, 10), 100)
    .action(async (cmdOpts: { url: string; canary: boolean; user: string; sku: string; quantity: number; amount: number }) => {
      const r = await sendOrder(cmdOpts);
      process.stdout.write(JSON.stringify({
        request: { url: `${cmdOpts.url}/api/orders`, canary: cmdOpts.canary },
        response: { status: r.status, data: r.data },
      }, null, 2) + "\n");
      if (r.status >= 400) process.exit(1);
    });

  await program.parseAsync(argv);
}
