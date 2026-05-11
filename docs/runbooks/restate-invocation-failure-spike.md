# Restate invocation failure spike

## Symptom
On the Canary — Substrates dashboard with `substrate=restate`, the error-rate panel shows a sustained spike for one or more handlers. Equivalently:

```
sum by (target, lane) (rate(canary_request_total{substrate="restate", outcome!="success"}[5m])) > 0
```

returns rows.

## Likely causes
- A handler-side exception is escaping the `try/catch` inside the `measure(...)` wrapper (real bug — the wrapper records the failure and re-throws).
- Restate runtime journal corruption or a state-machine constraint violation (rare but visible as `outcome=error` with a Restate-internal exception class).
- A downstream call (HTTP, Kafka) inside the handler body is failing, causing the handler to throw.
- Kafka topic schema mismatch when the canary handler is on a new schema and stable is on the old one.

## Diagnosis
1. From the dashboard, note the offending `target` (e.g. `PaymentVOCanary.charge`).
2. Open `Canary — Traces`, set `service` to the matching Java/Node service and `lane` to `canary`. Find a recent failed trace.
3. Inspect the trace span for the handler — its events should contain the exception class and message.
4. If trace propagation is incomplete (Phase 5.b verification was deferred — see Task 10 of `2026-05-11-canary-release-phase-5-b-trace-propagation.md`), fall back to pod logs:
   ```
   kubectl -n services logs -l app=<service>,version=canary --tail=500 | grep -i 'restate\|exception'
   ```
5. Check Restate admin for journal state of recent invocations:
   ```
   kubectl -n restate exec restate-0 -- restatectl invocations list --status killed,failed
   ```

## Mitigation
1. If the bug is in canary code: `canary-ctl rollback <service>` and address in the next image build.
2. If a journal is wedged on a single invocation: cancel it via `restatectl invocations cancel <id>` after capturing the journal for postmortem.
3. If the spike is caused by transient downstream (Kafka topic moving leaders, stable handler not yet up): wait one full bake interval and re-evaluate; if the spike persists, treat as a real failure.

## Resolution / postmortem hooks
- Save the trace ID(s) and any captured journal payloads.
- File a ticket against the canary handler if a real bug is found.
- If trace propagation was missing, escalate to apply the manual `traceparent` injection paths documented in 5.b plan Tasks 8/9.
