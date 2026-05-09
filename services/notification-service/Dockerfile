# syntax=docker/dockerfile:1
# Multi-stage Node build for notification-service.
# Build context is the repo root: docker build -f services/notification-service/Dockerfile -t canary-release-mgmt/notification-service:dev .

FROM node:25-alpine AS builder
WORKDIR /repo
RUN npm install -g pnpm@9.12.0
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json /repo/
COPY platform/lib-node/package.json /repo/platform/lib-node/
COPY platform/restate-defs-node/package.json /repo/platform/restate-defs-node/
COPY services/notification-service/package.json /repo/services/notification-service/
RUN pnpm install --frozen-lockfile
COPY platform/lib-node /repo/platform/lib-node
COPY platform/restate-defs-node /repo/platform/restate-defs-node
COPY services/notification-service /repo/services/notification-service
RUN pnpm --filter @canary/lib-node --filter @canary/restate-defs-node --filter @canary/notification-service build
RUN pnpm deploy --filter @canary/notification-service --prod /deploy

FROM node:25-alpine
WORKDIR /app
COPY --from=builder /deploy /app
ENTRYPOINT ["node", "dist/index.js"]
