import express, { type Express } from "express";
import axios, { type AxiosInstance } from "axios";
import { xCanaryMiddleware, attachXCanaryAxiosInterceptor } from "@canary/lib-node";
import type { NotifyRequest } from "@canary/restate-defs-node";
import { notificationStore, consumedEventStore } from "./store.js";

export interface HttpDeps {
  ingressClient: AxiosInstance;
}

export function buildIngressClient(ingressUrl: string): AxiosInstance {
  const client = axios.create({ baseURL: ingressUrl });
  attachXCanaryAxiosInterceptor(client);
  return client;
}

export function setupHttp(deps: HttpDeps): Express {
  const app = express();
  app.use(express.json());
  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use(xCanaryMiddleware);

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
