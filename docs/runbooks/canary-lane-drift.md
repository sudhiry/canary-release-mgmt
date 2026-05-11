# Canary lane drift

## Symptom
On the Canary — Overview dashboard, the lane-active matrix shows an unexpected value: a substrate row that should be `1` for both `stable` and `canary` shows one of them at `0` (or absent), or a service that has no canary deployed shows `canary=1`.

## Likely causes
- The canary Deployment or its Service Endpoints are unhealthy — pods crash-looping, readiness gate failing.
- The Kubernetes endpoint watcher (`LaneStateProbe`) lost its watch and didn't re-list (Java + Node both auto-recover; sustained drift suggests a deeper RBAC or networking issue).
- A manual `kubectl edit` left the cluster in a state that disagrees with `tools/canary-ctl` state.

## Diagnosis
1. Run:
   ```
   kubectl -n services get deploy,po,endpoints -l app=<service>
   ```
   Confirm the canary Deployment exists, its pods are Ready, and the Endpoints object lists them.
2. Compare against `canary-ctl status <service>`. The `helmCanaryPresent`, `deploymentReady`, and `vsHasHeaderRule` fields should agree with what the cluster reports.
3. Check the `LaneStateProbe` logs:
   ```
   kubectl -n services logs -l app=<service> --tail=200 | grep -i lane
   ```
   Look for `watch closed` or `re-list failed`.

## Mitigation
1. If a single pod is unhealthy: `kubectl -n services delete pod <pod>` to force a reschedule.
2. If the watcher is stuck: roll the affected service Deployment (`kubectl -n services rollout restart deploy/<service>-<lane>`).
3. If state and cluster disagree: run `canary-ctl rollback <service>` then `canary-ctl deploy-canary <service> <tag>` to re-converge.

## Resolution / postmortem hooks
- Capture the timeline (when did the gauge flip, when was the rollout, when was the manual edit if any).
- If the watcher was the cause, file a ticket against `lib-java`/`lib-node` `LaneStateProbe` to harden re-list.
