import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  auditQueryServiceDef,
  notificationServiceDef,
  type Notification,
  type NotifyRequest,
  type AuditEvent,
} from "@canary/restate-defs-node";
import { notificationStore } from "./store.js";
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

// Export the handler directly for unit testing
export async function notifyHandler(ctx: restate.Context, req: NotifyRequest): Promise<Notification> {
  // Read x-canary from invocation metadata.
  // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
  const isCanary = ctx.request().headers.get("x-canary") === "true";

  return runWithCanary(isCanary, async () => {
    const notification: Notification = {
      id: randomUUID(),
      userId: req.userId,
      message: req.message,
      status: "sent",
    };
    notificationStore.put(notification);

    if (kafkaSend) {
      await kafkaSend("notifications.events", notification.id, JSON.stringify(notification));
    }

    const auditEvent: AuditEvent = {
      aggregate: "notification",
      id: notification.id,
      action: "sent",
      correlationId: req.orderId,
    };
    await ctx.serviceClient(auditQueryServiceDef).append(
      auditEvent,
      restate.rpc.opts(applyXCanaryToRestateOptions({})),
    );

    return notification;
  });
}

export const notificationService = restate.service({
  name: notificationServiceDef.name,
  handlers: {
    notify: notifyHandler,
  },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  await restate.endpoint().bind(notificationService).listen(opts.port);
  console.log(`notification-service Restate handlers listening on ${opts.port}`);
}
