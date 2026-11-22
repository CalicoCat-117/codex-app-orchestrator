---
name: codex-app-orchestrator
description: >-
  Orchestrate one or more persistent, UI-visible Codex execution sessions via the
  official local `codex app-server` WebSocket JSON-RPC daemon. Use when an agent
  acts as supervisor/director and delegates implementation, debugging, analysis,
  or iterative research execution to Codex threads while preserving live Codex
  Desktop/VSCode visibility, Guardian `auto_review` policy, non-blocking mid-turn
  steering (`turn/steer`), multi-session worktree isolation, periodic check-ins,
  and quota-aware failover.
---

# Codex App Orchestrator

Delegate concrete execution to one or more persistent Codex sessions while
keeping every session live and inspectable in the user's Codex Desktop / IDE UI.

All control flows through the zero-dependency helper
[scripts/codex_app_ctl.py](./scripts/codex_app_ctl.py) over the official local
`codex app-server` Unix WebSocket socket. No extra MCP server or Python package
installation is required.

## Core Operating Rules

1. **Always keep sessions UI-visible.**
   - Create and drive sessions through `codex app-server` (`create`, `dispatch`,
     `launch-batch`) or `codex queue --thread <ID> --resume`.
   - **Never** use headless `codex exec` / `codex exec resume` for user-monitored
     execution — headless runs hide live activity from the Codex UI.
2. **Use Guardian `auto_review` by default.**
   - Default policy for autonomous execution is `approvalPolicy: "on-request"`,
     `approvalsReviewer: "auto_review"`, `sandbox: "workspace-write"` (with
     `networkAccess: false`).
   - Do not switch to `never` / `danger-full-access` unless the user explicitly
     authorizes it.
   - Verify effective policy on real turns via rollout `turn_context`
     (`status --verify-policy` or `inspect-rollout`), because already-loaded
     threads only reflect updated policy overrides on their next `turn/start`.
3. **Never let directives stall behind an active turn.**
   - Use `dispatch` for every message: it inspects live turn state and
     automatically routes:
     - `inProgress` turn → **`turn/steer`** with `expectedTurnId` (immediate
       mid-turn injection; falls back to `turn/start` if the turn finishes mid-race)
     - `notLoaded` thread → **`thread/resume`** then **`turn/start`**
     - `idle` thread → **`turn/start`** with explicit policy/sandbox/model overrides
   - Instruct every executor to follow the **Turn-Yielding Protocol** (launch
     async jobs or timers, record state, and end the turn immediately — never
     spin in a `sleep()` loop inside a turn).
4. **Enforce strict multi-session isolation.**
   - Give every parallel writer session its own dedicated `git worktree`, unique
     branch, and exclusive `output_dir`.
   - Designate **at most one** session as the shared ops/integration/board writer;
     keep parallel design/feature sessions strictly inside their own worktrees
     and read-only analysts out of all git working trees.
   - Track the fleet in a single `FLEET-REGISTRY.json` manifest.
5. **Verify from artifacts, respect quota and polling hygiene.**
   - Check daemon and quota health (`health` or `--check-quota`) before large
     dispatches. If `ordinaryUsageAllowed` is `false`, do not spam retries —
     either pause and report to the user or fail over if the user granted
     failover authority.
   - Audit local receipts, git commits, and manifests before querying remote
     clusters or APIs; space any external/shared-server telemetry queries by
     at least 10 seconds (prefer 15–60s+ or milestone-aligned wakeups).
6. **Return plain, principle-first session captions.**
   - Keep session updates and handoffs easy to read and understated: explain the
     underlying mechanism, root cause, or design rationale (*why* a change works
     or failed) alongside concrete observed outcomes (files, commits, metrics,
     or open unknowns).
   - Avoid decorative emojis, hype phrasing, and repetitive gatekeeping or
     compliance checklists—only surface policy, sandbox, or error notes when a
     real deviation or blocker occurs (or when explicitly requested).
   - `create` and `launch-batch` attach this lightweight caption guide to
     `developerInstructions` by default (disable with `--no-caption-guide` or
     `"caption_guide": false`), and `caption` extracts and formats clean plain
     captions from live sessions or rollouts.

## Standard Workflow

### 1. Preflight Health & Discovery

Check daemon connectivity, configured default model/effort, quota status, and
existing sessions:

```bash
python3 scripts/codex_app_ctl.py health --pretty
python3 scripts/codex_app_ctl.py list --search "topic" --pretty
```

If the daemon is not running, start it with `codex app-server daemon start` and
re-run `health`.

`health.models` compares the configured default model with the newest model the
server offers in the same family (`gpt-6.2-sol` → the highest `gpt-*-sol`). When
`configuredIsNewest` is `false`, tell the user and, with their OK, update `model`
in `~/.codex/config.toml`. `~/.codex/config.toml` is the single source of model
and effort: leave `--model` / `--effort` unset on `create`, `dispatch`, and fleet
specs, including quick smoke tests, unless the user asks for a specific value.

### 2. Launch a Single Executor or Parallel Fleet

**Single session** (inherits `~/.codex/config.toml` model/effort; do not override them unasked):

```bash
python3 scripts/codex_app_ctl.py create \
  --name "Worker · Feature X" \
  --cwd /abs/path/to/workspace \
  --prompt-file /abs/path/to/directive.md \
  --check-quota --pretty
```

**Multi-session fleet** (create isolated `git worktree`s first, validate with
`--dry-run`, then launch and write a registry):

```bash
python3 scripts/codex_app_ctl.py launch-batch \
  --spec examples/fleet-spec.example.json --dry-run --pretty

python3 scripts/codex_app_ctl.py launch-batch \
  --spec /abs/path/to/fleet-spec.json \
  --registry-out /abs/path/to/FLEET-REGISTRY.json \
  --check-quota --pretty
```

Before any RPC call, `launch-batch` rejects a spec when two writer slots share a
write root (their `worktree`, or `cwd` when a slot has no worktree), two slots
claim the same `branch`, `output_dir`s are equal or nested, slot numbers repeat,
or more than one slot is `cluster_write_owner`. Errors come back as a JSON object
with exit code 1. Nested writer worktrees, writers without a worktree, and
`danger-full-access` / `never` policies pass but are listed under `warnings`
(the dry-run output and the registry's `validation_warnings`); read them before
launching.

### 3. Monitor, Render Captions, Verify Policy, and Steer/Dispatch

Inspect the whole fleet in one lightweight O(1) pass (uses `thread/turns/list`
with `limit=1`, avoiding full-history downloads), or render a readable,
unembellished caption directly:

```bash
python3 scripts/codex_app_ctl.py status \
  --registry /abs/path/to/FLEET-REGISTRY.json \
  --verify-policy --last-message --pretty

# Plain, principle-first session caption (supports thread IDs, --registry, or offline --path)
python3 scripts/codex_app_ctl.py caption --registry /abs/path/to/FLEET-REGISTRY.json
python3 scripts/codex_app_ctl.py caption <THREAD_UUID> --json --pretty

# Raw rollout facts: effective turn_context, last turn state, recent errors
python3 scripts/codex_app_ctl.py inspect-rollout --thread <THREAD_UUID> --pretty
```

Deliver a new round directive or mid-turn correction without blocking:

```bash
python3 scripts/codex_app_ctl.py dispatch \
  --thread <THREAD_UUID> \
  --message-file /abs/path/to/round-directive.md \
  --check-quota --pretty
```

Use `--steer-only` when an instruction should only inject into a currently
running turn and be skipped if the thread already finished. Use `--queue-only`
only when you intentionally want `codex queue --resume` deferred delivery.

### 4. Interrupt or Retire Sessions

```bash
# Gracefully stop a misdirected active turn
python3 scripts/codex_app_ctl.py interrupt --thread <THREAD_UUID> --pretty

# Archive finished sessions (keeps history, cleans sidebar) or delete temp ones
python3 scripts/codex_app_ctl.py archive <THREAD_UUID>
python3 scripts/codex_app_ctl.py delete <TEMP_THREAD_UUID>
```

### 5. Periodic Supervision Loop

When running continuous multi-round research or engineering:

1. Align each supervision check-in to a real milestone (e.g., expected job
   checkpoint or 20–45 min cadence) using the host agent's scheduler/timer.
2. Perform a **local-first audit**: run `status --registry ... --verify-policy`,
   check git commits/diffs in each worktree, and read emitted result artifacts
   or manifests. Claim completion only from verified artifacts, never from turn
   or scheduler status alone.
3. Synthesize findings, decide the next round's directions, write concise
   directive files, and call `dispatch` for each slot that needs new work or
   mid-turn steering.
4. If `health` / `--check-quota` returns exit code `3` (`quota_exhausted`),
   follow the failover / handoff-back protocol in
   [references/fleet-and-supervision-patterns.md](./references/fleet-and-supervision-patterns.md).

## Validation Checklist

After creating or updating sessions, verify:

- `health` reports `daemon.status == "running"` and `quota.ordinaryUsageAllowed == true`.
- `health.models.configuredIsNewest` is not `false`. If it is, a newer model of the
  configured family exists; raise it with the user before launching more work.
- `status --verify-policy` shows `rollout_turn_context.approval_policy == "on-request"`,
  `approvals_reviewer == "auto_review"`, and expected `model` / `effort` / `sandbox_policy`.
- Every writer slot in `FLEET-REGISTRY.json` points to a distinct worktree/branch
  and output directory, and commits cleanly without touching peer worktrees.

## Reference Guides & Templates

- Protocol details, RPC schemas, Guardian policy mechanics, and troubleshooting:
  [references/protocol-and-policy.md](./references/protocol-and-policy.md)
- Multi-session isolation, Turn-Yielding boilerplate, supervision cadence, and
  quota failover/handoff:
  [references/fleet-and-supervision-patterns.md](./references/fleet-and-supervision-patterns.md)
- Generic fleet specification example:
  [examples/fleet-spec.example.json](./examples/fleet-spec.example.json)
- Generic directive & handoff template:
  [examples/directive.example.md](./examples/directive.example.md)
