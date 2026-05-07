# canary-release-mgmt

A reference architecture for canary release management across HTTP, Kafka,
and Restate.dev in a polyglot microservice system.

This repository is built in phases. See `docs/superpowers/specs/` for design
specs and `docs/superpowers/plans/` for implementation plans.

## Phase 1 — Substrate + HTTP canary

Quickstart:

    make up         # bootstrap kind + Istio + Kafka + Restate
    make smoke-infra # verify all infra is Ready
    make down       # tear down

Full design: `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`
