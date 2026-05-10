import { describe, it, expect, beforeAll, afterAll } from "vitest";
import axios from "axios";
import { ensureCleanBaseline } from "./helpers/cluster.js";
import { sendOrder } from "./helpers/traffic.js";
import {
  openSubsetForward,
  getConsumedEvents,
  waitForConsumed,
} from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";

const RUN_SLOW = process.env.RUN_SLOW_E2E === "true";
const RESTATE_INGRESS_URL =
  process.env.RESTATE_INGRESS_URL ?? "http://localhost:9070";

describe("R1–R3 — Restate saga happy path + compensation", () => {
  let auditStable: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    auditStable = await openSubsetForward("audit-service", "stable");
  }, 120_000);

  afterAll(async () => {
    await auditStable?.stop();
  });

  it("R1 — saga happy path: all four R-to-R steps fire; reservation confirmed", async () => {
    const resp = await sendOrder({ user: "r1-happy" });
    expect(resp.status).toBeGreaterThanOrEqual(200);
    expect(resp.status).toBeLessThan(300);

    const rows = await waitForConsumed(
      auditStable,
      (r) => {
        const text = r.map((x) => x.value).join("\n");
        return (
          /reserved/.test(text) &&
          /charged/.test(text) &&
          /confirmed/.test(text) &&
          /sent/.test(text)
        );
      },
      30_000,
    );
    expect(rows.length).toBeGreaterThanOrEqual(4);
  }, 60_000);

  it("R2 — payment compensation: payment refuses negative amount → reservation released", async () => {
    const resp = await sendOrder({ user: "r2-comp", amount: -1 });
    expect(resp.status).toBe(502);

    const rows = await waitForConsumed(
      auditStable,
      (r) => {
        const text = r.map((x) => x.value).join("\n");
        return /reserved/.test(text) && /released/.test(text);
      },
      30_000,
    );
    const text = rows.map((x) => x.value).join("\n");
    expect(text).toMatch(/reserved/);
    expect(text).toMatch(/released/);
    expect(text).not.toMatch(/charged/);
    expect(text).not.toMatch(/confirmed/);
  }, 60_000);

  it("R3 — notify compensation: notify refuses → payment refunded; reservation stays confirmed", async () => {
    const resp = await sendOrder({ user: "reject-me" });
    expect(resp.status).toBe(502);

    const rows = await waitForConsumed(
      auditStable,
      (r) => {
        const text = r.map((x) => x.value).join("\n");
        return (
          /charged/.test(text) &&
          /refunded/.test(text) &&
          /confirmed/.test(text)
        );
      },
      30_000,
    );
    const text = rows.map((x) => x.value).join("\n");
    expect(text).toMatch(/refunded/);
    expect(text).toMatch(/confirmed/);
    const orderIdMatch = text.match(
      /"correlationId":"([^"]+)"[^\n]*"action":"confirmed"/,
    );
    if (orderIdMatch) {
      const orderId = orderIdMatch[1];
      const releaseForThisOrder = new RegExp(
        `"correlationId":"${orderId}"[^\\n]*"action":"released"`,
      );
      expect(text).not.toMatch(releaseForThisOrder);
    }
  }, 60_000);
});

(RUN_SLOW ? describe : describe.skip)(
  "R4–R5 — Restate substrate slow paths",
  () => {
    let auditStable: PodPortForward;

    beforeAll(async () => {
      await ensureCleanBaseline();
      auditStable = await openSubsetForward("audit-service", "stable");
    }, 120_000);

    afterAll(async () => {
      await auditStable?.stop();
    });

    it(
      "R4 — reservation timer expiry transitions to expired",
      async () => {
        const orderId = "r4-timer-test-" + Date.now();
        const res = await axios.post(
          `${RESTATE_INGRESS_URL}/ReservationWorkflow/${orderId}/run`,
          { sku: "widget", quantity: 1, orderId },
          {
            headers: { "content-type": "application/json" },
            validateStatus: () => true,
          },
        );
        expect(res.status).toBeGreaterThanOrEqual(200);
        expect(res.status).toBeLessThan(300);

        // Timer is 120s; allow some slack for tick + propagation.
        await new Promise((r) => setTimeout(r, 130_000));

        const rows = await waitForConsumed(
          auditStable,
          (r) => {
            const text = r.map((x) => x.value).join("\n");
            return new RegExp(
              `"correlationId":"${orderId}"[^\\n]*"action":"expired"`,
            ).test(text);
          },
          30_000,
        );
        expect(rows.length).toBeGreaterThan(0);
      },
      300_000,
    );

    it(
      "R5 — refund idempotency: invoke refund twice, expect single refund event",
      async () => {
        const resp = await sendOrder({ user: "r5-idempotency" });
        expect(resp.status).toBe(201);

        const orderId = (resp.data as { id: string }).id;

        const refundBody = { orderId, amount: 100 };
        const refundCfg = {
          headers: { "content-type": "application/json" },
          validateStatus: () => true,
        };
        await axios.post(
          `${RESTATE_INGRESS_URL}/PaymentVO/${orderId}/refund`,
          refundBody,
          refundCfg,
        );
        await axios.post(
          `${RESTATE_INGRESS_URL}/PaymentVO/${orderId}/refund`,
          refundBody,
          refundCfg,
        );

        // Allow Kafka audit fan-out to complete.
        await new Promise((r) => setTimeout(r, 5_000));

        const rows = await getConsumedEvents(auditStable);
        const text = rows.map((x) => x.value).join("\n");
        const refundEvents =
          text.match(
            new RegExp(
              `"correlationId":"${orderId}"[^\\n]*"action":"refunded"`,
              "g",
            ),
          ) ?? [];
        expect(refundEvents.length).toBe(1);
      },
      120_000,
    );
  },
);
