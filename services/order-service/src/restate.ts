import * as restate from "@restatedev/restate-sdk";
import { runWithCanary } from "@canary/lib-node";
import {
  checkoutSagaDef,
  type Order,
  type OrderRequest,
} from "@canary/restate-defs-node";
import { randomUUID } from "node:crypto";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

/**
 * 1.3.a stub: registered so the canary flag (RESTATE_REGISTER_HANDLERS) has
 * something to gate. Phase 3 fills in the actual saga logic with R-to-R calls
 * to PaymentVO, ReservationWorkflow, NotificationService.
 */
export async function checkoutSagaRunHandler(ctx: restate.WorkflowContext, req: OrderRequest): Promise<Order> {
  // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
  const isCanary = ctx.request().headers.get("x-canary") === "true";

  return runWithCanary(isCanary, async () => {
    // Phase 3 will replace this body with R-to-R calls to
    // PaymentVO, ReservationWorkflow, NotificationService.
    return {
      id: randomUUID(),
      userId: req.userId,
      sku: req.sku,
      quantity: req.quantity,
      amount: req.amount,
      status: "stub-completed",
    };
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
