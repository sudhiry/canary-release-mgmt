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
import type { NotifyRequest } from "@canary/restate-defs-node";
import { notificationStore, consumedEventStore } from "./store.js";

export interface HttpDeps {
  ingressClient: AxiosInstance;
  kafkaHealth?: KafkaHealthState;
  /** "stable" | "canary"; defaults to process.env.VERSION ?? "stable". Only canary's /health is gated on Kafka health. */
  version?: string;
}

export function buildIngressClient(ingressUrl: string): AxiosInstance {
  const client = axios.create({ baseURL: ingressUrl });
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

  app.post("/notifications", async (req, res) => {
    const body = req.body as NotifyRequest;
    try {
      const response = await deps.ingressClient.post(
        "/NotificationService/notify",
        body,
      );
      res.status(201).json(response.data);
    } catch (err) {
      console.error("ingress call failed", err);
      res.status(502).json({ error: "ingress_failed" });
    }
  });

  app.get("/notifications/by-user/:userId", (req, res) => {
    const found = notificationStore.byUserId(req.params.userId);
    res.json(found);
  });

  app.get("/internal/consumed-events", (_req, res) => {
    res.json(consumedEventStore.all());
  });

  return app;
}
