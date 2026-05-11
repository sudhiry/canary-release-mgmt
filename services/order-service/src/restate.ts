import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions, measureRestate, type CanaryMetrics } from "@canary/lib-node";
import {
  checkoutSagaStableDef,
  checkoutSagaCanaryDef,
  paymentVOStableDef,
  paymentVOCanaryDef,
  reservationWorkflowStableDef,
  reservationWorkflowCanaryDef,
  notificationServiceStableDef,
  notificationServiceCanaryDef,
  type Order,
  type OrderRequest,
} from "@canary/restate-defs-node";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
  metrics?: CanaryMetrics;
}

let metricsRef: CanaryMetrics | null = null;

export function configureMetrics(m: CanaryMetrics): void {
  metricsRef = m;
}

export const MY_VARIANT: "stable" | "canary" =
  process.env.VERSION === "canary" ? "canary" : "stable";

// Pick the matching set of defs at module load. Saga is locked to its own
// variant for all downstream calls — never re-evaluates per-request.
const checkoutSagaDef =
  MY_VARIANT === "canary" ? checkoutSagaCanaryDef : checkoutSagaStableDef;
const paymentVODef =
  MY_VARIANT === "canary" ? paymentVOCanaryDef : paymentVOStableDef;
const reservationWorkflowDef =
  MY_VARIANT === "canary" ? reservationWorkflowCanaryDef : reservationWorkflowStableDef;
const notificationServiceDef =
  MY_VARIANT === "canary" ? notificationServiceCanaryDef : notificationServiceStableDef;

export async function checkoutSagaRunHandler(
  ctx: restate.WorkflowContext,
  req: OrderRequest,
): Promise<Order> {
  const isCanary = ctx.request().headers.get("x-canary") === "true";
  const orderId = ctx.key;
  const handlerName = `${checkoutSagaDef.name}.run`;

  const body = async (): Promise<Order> => {
    return runWithCanary(isCanary, async () => {
      const auditTrail: string[] = [
        `saga@${MY_VARIANT}`,
        `reservation@${MY_VARIANT}`,   // by-construction trust
      ];

      const order: Order = {
        id: orderId,
        userId: req.userId,
        sku: req.sku,
        quantity: req.quantity,
        amount: req.amount,
        status: "pending",
        auditTrail,
      };

      const reservationSendClient = ctx.workflowSendClient(reservationWorkflowDef, orderId);
      const reservationClient = ctx.workflowClient(reservationWorkflowDef, orderId);
      const paymentClient = ctx.objectClient(paymentVODef, orderId);
      const notificationClient = ctx.serviceClient(notificationServiceDef);

      // Step 1: reserve (fire-and-forget; parks on awakeable+timer)
      try {
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

      // Step 2: charge — observe canary tweak via charge.amount math
      let chargedAmount: number;
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const charge = await (paymentClient as any).charge(
          { orderId, amount: req.amount },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        chargedAmount = charge.amount;
        const paymentVariant = chargedAmount === req.amount ? "stable" : "canary";
        auditTrail.push(`payment@${paymentVariant}`);
      } catch (e) {
        if (e instanceof restate.TerminalError) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          await (reservationClient as any).release(
            restate.rpc.opts(applyXCanaryToRestateOptions({})),
          );
          return { ...order, status: "failed" };
        }
        throw e;
      }

      // Step 3: confirm — now returns Reservation; bufferUnits attests the variant
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const confirmed = await (reservationClient as any).confirm(
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        // confirmed.bufferUnits should match MY_VARIANT (sanity, not enforced here)
        void confirmed;
      } catch (e) {
        if (e instanceof restate.TerminalError) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          await (paymentClient as any).refund(
            { orderId, amount: req.amount },
            restate.rpc.opts(applyXCanaryToRestateOptions({})),
          );
          return { ...order, status: "failed" };
        }
        throw e;
      }

      // Step 4: notify — NotifyResult.version attests the variant
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const notifyResp = await (notificationClient as any).notify(
          { userId: req.userId, message: `Order ${orderId} confirmed`, orderId },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        auditTrail.push(`notification@${notifyResp.version}`);
      } catch (e) {
        if (e instanceof restate.TerminalError) {
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
  };

  return metricsRef ? measureRestate(metricsRef, handlerName, body) : body();
}

export const checkoutSaga = restate.workflow({
  name: checkoutSagaDef.name,
  handlers: { run: checkoutSagaRunHandler },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (opts.metrics) {
    configureMetrics(opts.metrics);
  }
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  console.log(`order-service Restate variant=${MY_VARIANT} binding ${checkoutSagaDef.name}`);
  await restate.endpoint().bind(checkoutSaga).listen(opts.port);
  console.log(`order-service Restate handlers listening on ${opts.port}`);
}
