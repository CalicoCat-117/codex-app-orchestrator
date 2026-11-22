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
     - turn blocked on `request_user_input` (`activeFlags: ["waitingOnUserInput"]`)
       → **`turn/interrupt`** then **`turn/start`** with the answer, because a steer
       cannot answer the pending tool call
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
6. **Keep questions from stalling sessions.**
   - Unattended Codex sessions rarely block on a protocol-level prompt; they
     usually end a turn by handing a decision back, and the thread then sits
     idle until someone replies.
   - `create` and `launch-batch` attach an autonomy guard to
     `developerInstructions` by default (disable with `--no-autonomy-guard` or
     `"autonomy_guard": false`). It tells the executor to decide reversible
     choices itself, ask only before irreversible, out-of-scope, unauthorized,
     or goal-changing steps, finish independent work first, and ask through one
     `[DECISION-NEEDED]` block. Instructions that already define such a block
     keep their own wording.
   - `status` reports `attention` per thread: `waiting_on_user_input`,
     `waiting_on_approval`, `decision_needed` (with parsed `question`,
     `options`, `recommended`), or `question` (heuristic: the turn ended on a
     prose question). `status --attention-only` lists just those threads.
   - The turn summary of a `notLoaded` thread often leaves out the agent's
     closing message. `status` then reads the last assistant message from the
     thread's rollout, and `attention.message_source` says where the message
     came from (`turn` or `rollout`).
   - Between check-ins, run `watch` in the background. It returns as soon as a
     watched turn finishes or a thread needs attention, so the supervisor wakes
     on the event instead of the next timer.
   - Answer reversible engineering choices yourself with `dispatch`. Take
     decisions about data access, irreversible actions, external publication,
     scope or budget, and headline conclusions to the user.
   - Do not run unattended executors in Codex Plan mode: `request_user_input`
     is available there and blocks the turn.
7. **Return plain, principle-first session captions.**
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
8. **Archive sessions that have no follow-up.**
   - Once a session's results are verified and no further round or reply is
     planned for it, archive it with `retire`. The thread keeps its full history
     but leaves the sidebar, so the sessions still in the sidebar are the ones
     that are working or waiting on you.
   - `retire` archives only threads with nothing left: it skips a running turn,
     a pending question or decision (`attention`), and a failed or interrupted
     last turn, and reports each skip with a reason. `--force` covers the last
     two after you have handled them; a running turn is never archived
     (`interrupt` it first).
   - A thread that another Codex process holds open (an open Codex TUI, or the
     `codex app-server` child of the Claude Code Codex plugin's
     `app-server-broker.mjs`) cannot be archived. `retire` reports it as
     `skipped` with reason `held_by_other_process` and moves on; retire it
     again once that process lets go of it. `--dry-run` cannot detect this.
   - `retire` handles each thread on its own. Any other failure is reported as
     `action: "error"` for that thread, the rest of the batch still runs, and
     the command exits `1` if any thread failed.
   - Archiving moves more than the visible thread: the rollouts of the
     sub-agents it spawned (`thread_spawn`) go to `~/.codex/archived_sessions/`
     with it. On one install, retiring 220 threads moved about 2,600 sub-agent
     rollouts. Guardian `auto_review` reviewer rollouts stay in
     `~/.codex/sessions/`.
   - Archiving is reversible. `dispatch` to an archived thread unarchives it
     before resuming, so a later follow-up still works.
   - Use `delete` only for throwaway smoke-test threads.

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

Codex compresses older rollouts to `<rollout>.jsonl.zst` while the app-server
keeps reporting the `.jsonl` path. The helper reads the compressed copy through
the `zstd` command-line tool. If `zstd` is not installed, commands that need
such a rollout fail with `RolloutReadError`; install `zstd` rather than
treating the rollout as missing.

Deliver a new round directive or mid-turn correction without blocking:

```bash
python3 scripts/codex_app_ctl.py dispatch \
  --thread <THREAD_UUID> \
  --message-file /abs/path/to/round-directive.md \
  --check-quota --pretty
```

Use `--steer-only` when an instruction should only inject into a currently
running turn and be skipped if the thread already finished (or is blocked on
`request_user_input`, which a steer cannot answer). Use `--queue-only`
only when you intentionally want `codex queue --resume` deferred delivery.

### 4. Interrupt or Retire Sessions

```bash
# Gracefully stop a misdirected active turn
python3 scripts/codex_app_ctl.py interrupt --thread <THREAD_UUID> --pretty

# Archive sessions with no follow-up (checked; preview with --dry-run)
python3 scripts/codex_app_ctl.py retire <THREAD_UUID>... --pretty
python3 scripts/codex_app_ctl.py retire --registry /abs/path/to/FLEET-REGISTRY.json --dry-run

# Unchecked archive, or delete throwaway smoke-test threads
python3 scripts/codex_app_ctl.py archive <THREAD_UUID>
python3 scripts/codex_app_ctl.py delete <TEMP_THREAD_UUID>
```

Each `retire` line reports `action`: `archived`, `would_archive`, `skipped`
with a `reason` (such as `turn_in_progress`, `needs_attention:decision_needed`,
`last_turn_failed`, `already_archived`, or `held_by_other_process`), or `error`
with the failure `detail`. `retire` exits `1` when any thread errored.
`status` shows `archived` per thread.

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
   mid-turn steering. Answer every thread with `attention` set (see rule 6),
   escalating the decisions that belong to the user. `retire` each slot that
   gets no next-round work (see rule 8), and retire the whole registry when the
   run ends.
4. Between check-ins, keep a `watch` running in the background so a finished
   turn or a new question wakes you immediately (exit `0` with `events`; exit
   `4` when `--max-wait` passes with nothing new):

   ```bash
   python3 scripts/codex_app_ctl.py watch \
     --registry /abs/path/to/FLEET-REGISTRY.json --interval 30 --max-wait 2700
   ```

   Add `--ignore-current` when you deliberately leave a thread waiting (for
   example, its question is with the user) so the same attention does not
   return the watch again.
5. If `health` / `--check-quota` returns exit code `3` (`quota_exhausted`),
   follow the failover / handoff-back protocol in
   [references/fleet-and-supervision-patterns.md](./references/fleet-and-supervision-patterns.md).

## Validation Checklist

After creating or updating sessions, verify:

- `health` reports `daemon.status == "running"` and `quota.ordinaryUsageAllowed == true`.
- `status --attention-only` prints nothing, or every listed thread has been
  answered or escalated to the user.
- Sessions with no further work planned have been retired (`status` shows
  `archived: true`), so the sidebar holds only working or waiting sessions.
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
