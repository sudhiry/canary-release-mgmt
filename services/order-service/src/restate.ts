import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  checkoutSagaDef,
  paymentVODef,
  reservationWorkflowDef,
  notificationServiceDef,
  type Order,
  type OrderRequest,
} from "@canary/restate-defs-node";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

/**
 * Real CheckoutSaga (Phase 3.a Task 7).
 *
 * Orchestrates the order checkout via Restate R-to-R calls with full
 * compensation. The workflow key (== orderId) is set by the caller via
 * the Restate Ingress URL `/CheckoutSaga/<orderId>/run`.
 *
 * Steps + compensation contract:
 *   1. Reserve  (ReservationWorkflow.run via workflowSendClient — fire-and-forget,
 *      since run() parks on awakeable+timer until confirm/release/expire).
 *   2. Charge   (PaymentVO.charge). On TerminalError → release reservation, return failed.
 *   3. Confirm  (ReservationWorkflow.confirm). On TerminalError (timer-raced)
 *      → refund payment, return failed. (No release; reservation already terminal.)
 *   4. Notify   (NotificationService.notify). On TerminalError → refund payment ONLY,
 *      reservation stays confirmed (partial reversal). Return failed.
 *
 * x-canary propagation: every R-to-R call passes an opts wrapper produced by
 * applyXCanaryToRestateOptions, which copies the canary flag from the AsyncLocal
 * context (set by runWithCanary) onto the per-call headers.
 */
export async function checkoutSagaRunHandler(
  ctx: restate.WorkflowContext,
  req: OrderRequest,
): Promise<Order> {
  // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
  const isCanary = ctx.request().headers.get("x-canary") === "true";
  // The workflow key is the orderId (set by the Ingress URL). WorkflowContext
  // extends ObjectContext which exposes `key: string`.
  const orderId = ctx.key;

  return runWithCanary(isCanary, async () => {
    const order: Order = {
      id: orderId,
      userId: req.userId,
      sku: req.sku,
      quantity: req.quantity,
      amount: req.amount,
      status: "pending",
    };

    const reservationSendClient = ctx.workflowSendClient(reservationWorkflowDef, orderId);
    const reservationClient = ctx.workflowClient(reservationWorkflowDef, orderId);
    const paymentClient = ctx.objectClient(paymentVODef, orderId);
    const notificationClient = ctx.serviceClient(notificationServiceDef);

    // Step 1: reserve. ReservationWorkflow.run parks on awakeable+timer until
    // confirm/release/expire — we MUST NOT await it (would deadlock since
    // confirm/release are sent from this same saga). workflowSendClient is the
    // SDK's fire-and-forget submission API; it returns an InvocationHandle
    // synchronously after Restate accepts the submission.
    try {
      // SendClient call signatures: `(arg, opts?) => InvocationHandle`. The Restate
      // SDK's mapped `SendClient<M>` type (rpc.d.ts) collapses (arg, opts?) signatures
      // for handlers that take an arg. Cast to bypass the gap; runtime accepts opts.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (reservationSendClient as any).run(
        { sku: req.sku, quantity: req.quantity, orderId },
        restate.rpc.sendOpts(applyXCanaryToRestateOptions({})),
      );
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 2: charge.
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (paymentClient as any).charge(
        { orderId, amount: req.amount },
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // Compensation: release reservation. The workflow's parked run() will
        // see the release signal and transition to `released`.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (reservationClient as any).release(
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 3: confirm reservation. Resolves the parked awakeable with "confirm".
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (reservationClient as any).confirm(
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // Confirm raced with the 120s timer expiry — reservation is already
        // `expired`; we cannot confirm. Refund the payment and bail.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (paymentClient as any).refund(
          { orderId, amount: req.amount },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 4: notify.
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (notificationClient as any).notify(
        { userId: req.userId, message: `Order ${orderId} confirmed`, orderId },
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // Compensation: refund only. Reservation stays `confirmed` — the
        // partial-reversal contract from the spec. No release call.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (paymentClient as any).refund(
          { orderId, amount: req.amount },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    return { ...order, status: "completed" };
  });
}

export const checkoutSaga = restate.workflow({
  name: checkoutSagaDef.name,
  handlers: { run: checkoutSagaRunHandler },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  await restate.endpoint().bind(checkoutSaga).listen(opts.port);
  console.log(`order-service Restate handlers listening on ${opts.port}`);
}
