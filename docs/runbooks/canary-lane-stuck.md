# Canary lane stuck (past bake window)

## Symptom
The canary has been running past its intended bake window. Both lanes show traffic on the dashboard but `canary-ctl status <service>` shows `statePhase=active` indefinitely.

## Likely causes
- The operator workflow (deploy → observe → promote OR rollback) was abandoned mid-flight.
- A scheduled promotion script failed silently.
- The promoting actor is waiting for a signal that never came (e.g. dashboard didn't reach a clean window).

## Diagnosis
1. Run `canary-ctl status <service>` and confirm `statePhase` and the timestamp on `stateTag`.
2. Check `Canary — Overview` and `Canary — Substrates` to confirm whether the canary is healthy enough to promote.
3. Search shell history / CI logs for the planned promotion job to see whether it errored.

## Mitigation
1. If the canary is healthy: promote with the documented promotion path (typically `canary-ctl promote <service>` if available; otherwise re-deploy the canary tag as the stable tag and `rollback` the canary leg).
2. If the canary should not be promoted: `canary-ctl rollback <service>`.
3. Either way, leaving the cluster in `active` indefinitely keeps two image versions live and consumes capacity.

## Resolution / postmortem hooks
- Add the missed-promotion incident to the postmortem with a recommendation: either codify a hard bake-window timeout in tooling, or set up a calendar reminder.
- If the promotion script failed silently, file a ticket to add a non-zero exit + alert.
