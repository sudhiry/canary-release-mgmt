# Canary burning budget

## Symptom
The Canary — Overview dashboard shows the canary lane's error rate or p95 latency clearly worse than stable for one or more services. The gap is sustained (>2 minutes) and not a single transient spike.

## Likely causes
- A regression in the canary image (handler bug, dependency upgrade, schema change).
- A capacity gap — canary has fewer replicas than stable and is being saturated.
- A configuration drift — canary picked up a different env var or feature flag.
- A downstream substrate (Kafka topic, Restate service) is misrouting one lane.

## Diagnosis
1. Open `Canary — Substrates` and switch `substrate` to each of `http`, `kafka`, `restate`. Note which substrate the gap is concentrated in.
2. Run in Prometheus:
   ```
   sum by (service, target, outcome) (
     rate(canary_request_total{lane="canary", outcome!="success"}[5m])
   ) > 0
   ```
   The rows tell you which `target` is failing and the dominant `outcome` class.
3. Open `Canary — Traces`, set `service` to the offending service and `lane` to `canary`. Open a few traces and inspect span events for stack traces or downstream errors.
4. Check pod logs:
   ```
   kubectl -n services logs -l app=<service>,version=canary --tail=200
   ```

## Mitigation
1. **Reduce blast radius first** — if a percent-split mechanism existed, lower the canary share. (Phase 4 was skipped; the current cutover is header-routed, so canary is bounded to clients passing the header.)
2. **Roll back the canary** via `tools/canary-ctl/bin/canary-ctl rollback <service>`. This removes the canary Deployment + VirtualService rule cleanly.
3. If a substrate-level bug is suspected (Restate handler stuck, Kafka topic mis-keyed), confirm via the substrate dashboard before rollback so the postmortem captures the right root cause.

## Resolution / postmortem hooks
- Capture the failing query results + a sample trace ID into the postmortem doc.
- File a follow-up ticket if the canary image or its build pipeline produced the regression.
- Note in the postmortem whether the dashboard caught the burn within the bake window or whether an operator caught it later.
