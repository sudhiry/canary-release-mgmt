import express, { type Express } from "express";
import type { AxiosInstance } from "axios";
import {
  xCanaryMiddleware,
  xServedVersionMiddleware,
  xServedChainMiddleware,
  type KafkaHealthState,
} from "@canary/lib-node";
import type { Order, OrderRequest } from "@canary/restate-defs-node";
import { orderStore, consumedEventStore } from "./store.js";
import { randomUUID } from "node:crypto";

export interface HttpDeps {
  ingressClient: AxiosInstance;
  kafkaSend?: (topic: string, key: string, value: string) => Promise<void>;
  kafkaHealth?: KafkaHealthState;
  /** "stable" | "canary"; defaults to process.env.VERSION ?? "stable". Only canary's /health is gated on Kafka health. */
  version?: string;
}

export function setupHttp(deps: HttpDeps): Express {
  const app = express();
  app.use(express.json());
  const version = deps.version ?? process.env.VERSION ?? "stable";
  app.get("/health", (_req, res) => {
    // Only canary gates readiness on Kafka health. Stable must always
    // report ok (even with stale Kafka) to avoid the cold-cluster boot
    // deadlock — see deploy/helm/values/canary-overlay.yaml comment.
    if (version === "canary") {
      const report = deps.kafkaHealth?.report();
      if (report && !report.ok) {
        res.status(503).json({ ok: false, kafka: report });
        return;
      }
    }
    res.json({ ok: true });
  });
  app.use(xCanaryMiddleware);
  app.use(xServedVersionMiddleware());
  app.use(xServedChainMiddleware());

  app.post("/api/orders", async (req, res) => {
    const body = req.body as OrderRequest;
    // Generate orderId locally so we can include it in the Restate Ingress URL
    // (the workflow key) — matches the Java pattern in inventory's
    // ReservationController.create.
    const orderId = randomUUID();

    try {
      const result = await deps.ingressClient.post<Order>(
        `/CheckoutSaga/${orderId}/run`,
        body,
      );
      const order = result.data;
      orderStore.put(order);
      if (deps.kafkaSend) {
        await deps.kafkaSend("orders.events", order.id, JSON.stringify(order));
      }
      if (order.status === "completed") {
        res.status(201).json(order);
      } else {
        res.status(502).json({ error: "saga_failed", order });
      }
    } catch (err) {
      console.error("ingress invocation failed", err);
      const failed: Order = {
        id: orderId,
        userId: body.userId,
        sku: body.sku,
        quantity: body.quantity,
        amount: body.amount,
        status: "failed",
      };
      orderStore.put(failed);
      res.status(502).json({ error: "ingress_failed", order: failed });
    }
  });

  app.get("/api/orders/:id", (req, res) => {
    const order = orderStore.findById(req.params.id);
    if (!order) {
      res.status(404).end();
      return;
    }
    res.json(order);
  });

  app.get("/internal/consumed-events", (_req, res) => {
    res.json(consumedEventStore.all());
  });

  return app;
}
