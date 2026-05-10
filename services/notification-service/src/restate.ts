import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  notificationServiceStableDef,
  notificationServiceCanaryDef,
  auditQueryServiceDef,
  type NotifyRequest,
  type NotifyResult,
  type AuditEvent,
} from "@canary/restate-defs-node";
import { notificationStore, type StoredNotification } from "./store.js";
import { randomUUID } from "node:crypto";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

export type KafkaSend = (topic: string, key: string, value: string) => Promise<void>;

let kafkaSend: KafkaSend | null = null;

export function configureKafkaSend(fn: KafkaSend): void {
  kafkaSend = fn;
}

export const MY_VARIANT: "stable" | "canary" =
  process.env.VERSION === "canary" ? "canary" : "stable";

const notificationServiceDef =
  MY_VARIANT === "canary" ? notificationServiceCanaryDef : notificationServiceStableDef;

// Export the handler directly for unit testing
export async function notifyHandler(
  ctx: restate.Context,
  req: NotifyRequest,
): Promise<NotifyResult> {
  // Read x-canary from invocation metadata.
  // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
  const isCanary = ctx.request().headers.get("x-canary") === "true";

  return runWithCanary(isCanary, async () => {
    // Phase 3.a TerminalError driver preserved.
    if (req.userId === "reject-me") {
      throw new restate.TerminalError("notify rejected for test driver");
    }

    const id = randomUUID();

    const deliveredMessage =
      MY_VARIANT === "canary" ? `${req.message} [via canary notifier]` : req.message;

    const stored: StoredNotification = { id, userId: req.userId, message: deliveredMessage, status: "sent" };
    notificationStore.put(stored);

    if (kafkaSend) {
      await kafkaSend("notifications.events", id, JSON.stringify(stored));
    }

    const auditEvent: AuditEvent = {
      aggregate: "notification",
      id,
      action: "sent",
      correlationId: req.orderId,
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    // The Restate SDK's Client<M> type collapses (arg, opts?) to (opts?) for handlers that take one arg.
    // At runtime optsFromArgs() correctly handles (parameter, Opts) — cast to bypass the type gap.
    await (ctx.serviceClient(auditQueryServiceDef) as any).append(
      auditEvent,
      restate.rpc.opts(applyXCanaryToRestateOptions({})),
    );

    return {
      delivered: true,
      version: MY_VARIANT,
      deliveredMessage,
    };
  });
}

export const notificationService = restate.service({
  name: notificationServiceDef.name,
  handlers: { notify: notifyHandler },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  console.log(`notification-service Restate variant=${MY_VARIANT} binding ${notificationServiceDef.name}`);
  await restate.endpoint().bind(notificationService).listen(opts.port);
  console.log(`notification-service Restate handlers listening on ${opts.port}`);
}
