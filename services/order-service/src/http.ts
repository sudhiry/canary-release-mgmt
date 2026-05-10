import express, { type Express } from "express";
import axios, { type AxiosInstance } from "axios";
import {
  xCanaryMiddleware,
  xServedVersionMiddleware,
  xServedChainMiddleware,
  attachXCanaryAxiosInterceptor,
  attachXServedChainAxiosInterceptor,
  type KafkaHealthState,
} from "@canary/lib-node";
import type { Order, OrderRequest } from "@canary/restate-defs-node";
import { orderStore, consumedEventStore } from "./store.js";
import { runSaga, type SagaClients } from "./saga.js";
import { randomUUID } from "node:crypto";

export interface HttpDeps {
  clients: SagaClients;
  kafkaSend?: (topic: string, key: string, value: string) => Promise<void>;
  kafkaHealth?: KafkaHealthState;
  /** "stable" | "canary"; defaults to process.env.VERSION ?? "stable". Only canary's /health is gated on Kafka health. */
  version?: string;
}

export function buildClient(baseURL: string): AxiosInstance {
  const client = axios.create({ baseURL });
  attachXCanaryAxiosInterceptor(client);
  attachXServedChainAxiosInterceptor(client);
  return client;
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
    const orderId = randomUUID();

    const initial: Order = {
      id: orderId,
      userId: body.userId,
      sku: body.sku,
      quantity: body.quantity,
      amount: body.amount,
      status: "pending",
    };
    orderStore.put(initial);

    if (deps.kafkaSend) {
      await deps.kafkaSend("orders.events", orderId, JSON.stringify(initial));
    }

    try {
      await runSaga(orderId, body, deps.clients);
      const completed: Order = { ...initial, status: "completed" };
      orderStore.put(completed);
      res.status(201).json(completed);
    } catch (err) {
      const failed: Order = { ...initial, status: "failed" };
      orderStore.put(failed);
      console.error("saga failed", err);
      res.status(502).json({ error: "saga_failed", order: failed });
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
