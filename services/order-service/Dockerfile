# syntax=docker/dockerfile:1
# Multi-stage Node build for order-service.
# Build context is the repo root: docker build -f services/order-service/Dockerfile -t canary-release-mgmt/order-service:dev .

FROM node:25-alpine AS builder
WORKDIR /repo
RUN npm install -g pnpm@9.12.0
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json /repo/
COPY platform/lib-node/package.json /repo/platform/lib-node/
COPY platform/restate-defs-node/package.json /repo/platform/restate-defs-node/
COPY services/order-service/package.json /repo/services/order-service/
RUN pnpm install --frozen-lockfile
COPY platform/lib-node /repo/platform/lib-node
COPY platform/restate-defs-node /repo/platform/restate-defs-node
COPY services/order-service /repo/services/order-service
RUN pnpm --filter @canary/lib-node --filter @canary/restate-defs-node --filter @canary/order-service build
RUN pnpm deploy --filter @canary/order-service --prod /deploy

FROM node:25-alpine
WORKDIR /app
COPY --from=builder /deploy /app
ENTRYPOINT ["node", "dist/index.js"]
