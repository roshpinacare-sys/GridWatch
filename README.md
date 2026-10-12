# GridWatch — the fleet's control plane

The grid sees itself. Every 30 minutes (GitHub cloud — zero sandbox dependency):

1. **SENSE** — every repo, every workflow, every run: conclusion streaks, cadence from cron, silence detection, billing-block detection (via check-run annotations).
2. **DECIDE** — health classes: `ok | flaky | failing | billing-blocked | dormant | stale | never-run`.
3. **ACT** (autonomy envelope: reversible + measured only) — auto-retry failing/flaky via `workflow_dispatch` (max 3 consecutive, 12-min in-flight guard), re-enable `disabled_inactivity` workflows. Never fakes success.
4. **ALERT** — new red repos land in `FleetHQ/office/state/operator-inbox.jsonl` (agent role, honest evidence) and feed the sovereign decider.
5. **RECORD** — `state/fleet-health.json` is the single public truth file; double-pass verified after commit.

## Consumers
- The owner UI (worlds grid) reads `state/fleet-health.json` — the whole fleet's heartbeat in one place.
- `FleetHQ/tools/sovereign-decider.mjs` senses this file (P0 rule) and decides autonomously.

## Laws
- Agent role only; never writes as owner.
- Billing-blocked jobs are escalated, never retried (owner-level decision).
- 409-safe commits; retries once on race.
