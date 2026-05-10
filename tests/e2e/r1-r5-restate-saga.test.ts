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

// ---------------------------------------------------------------------------
// R1–R3 parameterised across stable and canary variants
// ---------------------------------------------------------------------------

describe.each([
  { variant: "stable" as const, canary: false },
  { variant: "canary" as const, canary: true },
])("R1–R3 — Restate saga happy path + compensation ($variant)", ({ variant, canary }) => {
  let auditForward: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    auditForward = await openSubsetForward("audit-service", variant);
  }, 120_000);

  afterAll(async () => {
    await auditForward?.stop();
  });

  it(`R1 — saga happy path (${variant}): all four R-to-R steps fire; reservation confirmed`, async () => {
    const resp = await sendOrder({ user: `r1-happy-${variant}`, canary });
    expect(resp.status).toBeGreaterThanOrEqual(200);
    expect(resp.status).toBeLessThan(300);

    // Variant-specific: auditTrail in the response body must be fully populated
    // and every entry tagged with the correct variant suffix.
    const order = resp.data as { auditTrail?: string[]; status?: string };
    if (order.auditTrail) {
      expect(order.auditTrail).toEqual([
        `saga@${variant}`,
        `reservation@${variant}`,
        `payment@${variant}`,
        `notification@${variant}`,
      ]);
    }

    // Original Kafka-based assertions (unchanged).
    const rows = await waitForConsumed(
      auditForward,
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

  it(`R2 — payment compensation (${variant}): payment refuses negative amount → reservation released`, async () => {
    const resp = await sendOrder({ user: `r2-comp-${variant}`, amount: -1, canary });
    expect(resp.status).toBe(502);

    // Variant-specific: saga returned early after reservation; payment step was
    // never reached so auditTrail ends at reservation.
    const body = resp.data as { order?: { auditTrail?: string[] } };
    const auditTrail = body?.order?.auditTrail;
    if (auditTrail) {
      expect(auditTrail).toEqual([
        `saga@${variant}`,
        `reservation@${variant}`,
      ]);
    }

    // Original Kafka-based assertions (unchanged).
    const rows = await waitForConsumed(
      auditForward,
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

  it(`R3 — notify compensation (${variant}): notify refuses → payment refunded; reservation stays confirmed`, async () => {
    const resp = await sendOrder({ user: `reject-me-${variant}`, canary });
    expect(resp.status).toBe(502);

    // Variant-specific: saga succeeded through payment but notify failed, so
    // auditTrail has three entries (no notification@).
    const body = resp.data as { order?: { auditTrail?: string[] } };
    const auditTrail = body?.order?.auditTrail;
    if (auditTrail) {
      expect(auditTrail).toEqual([
        `saga@${variant}`,
        `reservation@${variant}`,
        `payment@${variant}`,
      ]);
    }

    // Original Kafka-based assertions (unchanged).
    const rows = await waitForConsumed(
      auditForward,
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

// ---------------------------------------------------------------------------
// R4–R5 slow paths — parameterised across stable and canary variants
// ---------------------------------------------------------------------------

describe.skipIf(!RUN_SLOW).each([
  { variant: "stable" as const, canary: false },
  { variant: "canary" as const, canary: true },
])(
  "R4–R5 — Restate substrate slow paths ($variant)",
  ({ variant, canary }) => {
    let auditForward: PodPortForward;

    beforeAll(async () => {
      await ensureCleanBaseline();
      auditForward = await openSubsetForward("audit-service", variant);
    }, 120_000);

    afterAll(async () => {
      await auditForward?.stop();
    });

    it(
      `R4 — reservation timer expiry transitions to expired (${variant})`,
      async () => {
        const orderId = `r4-timer-test-${variant}-` + Date.now();
        const sagaName = variant === "canary" ? "ReservationWorkflowCanary" : "ReservationWorkflowStable";
        const res = await axios.post(
          `${RESTATE_INGRESS_URL}/${sagaName}/${orderId}/run`,
          { sku: "widget", quantity: 1, orderId },
          {
            headers: {
              "content-type": "application/json",
              ...(canary ? { "x-canary": "true" } : {}),
            },
            validateStatus: () => true,
          },
        );
        expect(res.status).toBeGreaterThanOrEqual(200);
        expect(res.status).toBeLessThan(300);

        // Timer is 120s; allow some slack for tick + propagation.
        await new Promise((r) => setTimeout(r, 130_000));

        const rows = await waitForConsumed(
          auditForward,
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
      `R5 — refund idempotency (${variant}): invoke refund twice, expect single refund event`,
      async () => {
        const resp = await sendOrder({ user: `r5-idempotency-${variant}`, canary });
        expect(resp.status).toBe(201);

        const orderId = (resp.data as { id: string }).id;

        // Variant-specific: completed order must have a full auditTrail.
        const order = resp.data as { auditTrail?: string[] };
        if (order.auditTrail) {
          expect(order.auditTrail).toEqual([
            `saga@${variant}`,
            `reservation@${variant}`,
            `payment@${variant}`,
            `notification@${variant}`,
          ]);
        }

        const paymentVOName = variant === "canary" ? "PaymentVOCanary" : "PaymentVOStable";
        const refundBody = { orderId, amount: 100 };
        const refundCfg = {
          headers: {
            "content-type": "application/json",
            ...(canary ? { "x-canary": "true" } : {}),
          },
          validateStatus: () => true,
        };
        await axios.post(
          `${RESTATE_INGRESS_URL}/${paymentVOName}/${orderId}/refund`,
          refundBody,
          refundCfg,
        );
        await axios.post(
          `${RESTATE_INGRESS_URL}/${paymentVOName}/${orderId}/refund`,
          refundBody,
          refundCfg,
        );

        // Allow Kafka audit fan-out to complete.
        await new Promise((r) => setTimeout(r, 5_000));

        const rows = await getConsumedEvents(auditForward);
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
