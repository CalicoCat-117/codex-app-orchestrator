# Codex App-Server Protocol & Policy Reference

This reference documents the wire protocol, RPC methods, policy semantics, and
troubleshooting patterns used by [scripts/codex_app_ctl.py](../scripts/codex_app_ctl.py).

## 1. Transport & Socket Discovery

The Codex Desktop / IDE UI connects to a local `codex app-server` daemon
speaking JSON-RPC over RFC 6455 WebSocket on a Unix domain socket (`AF_UNIX`).

`codex_app_ctl.py` resolves the socket path in strict priority order:

1. `--socket <path>` CLI argument
2. `CODEX_APP_SERVER_SOCKET` environment variable
3. `codex app-server daemon version` JSON output → `socketPath` (also verifies
   `status == "running"`)
4. Fallback `$CODEX_HOME/app-server-control/app-server-control.sock` (or
   `~/.codex/app-server-control/app-server-control.sock`)

### Connection Handshake

1. Open `AF_UNIX` stream socket to `socketPath`.
2. Send HTTP/1.1 WebSocket upgrade (`Upgrade: websocket`, `Sec-WebSocket-Version: 13`,
   random 16-byte base64 `Sec-WebSocket-Key`) and verify HTTP `101`.
3. Send JSON-RPC `initialize` request with `clientInfo` and
   `capabilities: {"experimentalApi": false}`, wait for its response `id`.
4. Send JSON-RPC notification `{"method": "initialized", "params": {}}`.
5. Ignore unsolicited server notifications (messages without a matching `id`),
   handle fragmented text frames (`FIN=0` + continuation opcode `0x0`), and
   reply to server `PING (0x9)` frames with masked `PONG (0xA)`.

## 2. Core RPC Methods Used

| RPC Method | Purpose | Notes |
| --- | --- | --- |
| `config/read` | Read user defaults (`model`, `model_reasoning_effort`, `approvals_reviewer`, `sandbox_mode`) | Avoids hardcoding machine-specific models or paths. |
| `account/rateLimits/read` | Check `ordinaryUsageAllowed`, `primary.usedPercent`, `resetsAt`, `rateLimitReachedType` | Enables proactive quota gating (`--check-quota`, exit code `3`). |
| `thread/list` | Search/list threads by `searchTerm`, `cwd`, `limit`, `archived` | Passes `useStateDbOnly: true` by default (falls back automatically on older servers) to avoid full-history JSONL rollout scans; pass `--scan-rollouts` to force scan-and-repair. A page holds at most 100 rows (0.169) whatever `limit` asks for, so `list` passes `nextCursor` back as `cursor` until it has `--limit` rows. `archived: true` returns only archived threads; omitted or `false` returns only active ones. |
| `thread/loaded/list` | List thread IDs currently resident in daemon memory | Fast membership check. |
| `thread/read` | Read thread metadata (`status`, `name`, `cwd`, `model`, `path`) | Pass `includeTurns: false` to stay O(1) on long sessions. |
| `thread/turns/list` | Fetch latest turn (`limit: 1, sortDirection: "desc"`) | Works on both loaded and `notLoaded` threads; returns active `id`, `status`, `error`, and summary `items`. The default `summary` items often leave out the agent message (`notLoaded` threads, compaction turns), so `status` falls back to the last assistant message in the rollout to judge `attention`. |
| `thread/start` | Create a persistent (`ephemeral: false`), UI-visible thread | Sets initial `cwd`, `model`, `config`, `approvalPolicy`, `approvalsReviewer`, `sandbox`. |
| `thread/name/set` | Set the human-readable sidebar title shown in the Codex UI | Call immediately after `thread/start`. |
| `thread/resume` | Load a `notLoaded` thread and optionally update thread defaults | Pass `excludeTurns: true` to avoid huge payloads. |
| `turn/start` | Start a new turn on an `idle` (loaded) thread | Accepts per-turn overrides (`model`, `effort`, `approvalPolicy`, `approvalsReviewer`, `sandboxPolicy`, `cwd`). |
| `turn/steer` | Inject input mid-turn into an `inProgress` turn | Requires `expectedTurnId`; prevents commands from stalling in the queue. |
| `turn/interrupt` | Gracefully interrupt an active turn | Takes `threadId` and active `turnId`. |
| `thread/archive` / `unarchive` / `delete` | Thread lifecycle cleanup | Archiving unloads the thread and moves its rollout to `~/.codex/archived_sessions/` (that is how `status` reports `archived`). `thread/resume` then fails with "session <id> is archived", so `dispatch` calls `thread/unarchive` first; archiving twice fails with "no rollout found". Use `retire` for finished work, `delete` for smoke tests. |

## 3. UI Visibility: Why `app-server` Beats `codex exec`

- `codex exec` and `codex exec resume` execute in a standalone headless process.
  The user cannot watch tool calls, diffs, or live progress in Codex Desktop or
  the IDE extension.
- Sessions created via `codex app-server` with `ephemeral: false` immediately
  appear under their `cwd` workspace in the Codex UI sidebar, persist rollouts
  to `$CODEX_HOME/sessions/`, and stream live updates when driven by
  `turn/start`, `turn/steer`, or `codex queue --thread <ID> --resume`.
- Caveat on `codex queue`: `codex queue --thread <ID> --resume` preserves the
  thread's existing model and parks the message in the follow-up queue if a turn
  is currently running. Prefer `codex_app_ctl.py dispatch` whenever immediate
  steering or explicit model/policy control is required.

## 4. Guardian `auto_review` Policy & Verification

### Recommended Autonomous Policy

```json
{
  "approvalPolicy": "on-request",
  "approvalsReviewer": "auto_review",
  "sandbox": "workspace-write",
  "sandboxPolicy": {
    "type": "workspaceWrite",
    "writableRoots": [],
    "networkAccess": false,
    "excludeSlashTmp": false,
    "excludeTmpdirEnvVar": false
  }
}
```

- `approvalsReviewer: "auto_review"` routes approval prompts (sandbox escapes,
  network access, escalated commands) to Codex's built-in Guardian reviewer
  subagent, which evaluates risk in context rather than blocking on manual UI
  clicks or blindly bypassing all guardrails (`never` / `danger-full-access`).
- For read-only analyst/auditor sessions, set `sandbox: "read-only"` (maps to
  `{"type": "readOnly", "networkAccess": false}`).

### Critical Gotcha: Updating Policy on Already-Loaded Threads

When a thread is already resident in daemon memory (`thread/loaded/list`),
calling `thread/resume` alone may keep its cached in-memory policy until the
thread unloads.

`codex_app_ctl.py dispatch` eliminates this pitfall by passing explicit
`approvalPolicy`, `approvalsReviewer`, and `sandboxPolicy` fields directly on
**every `turn/start` call** (which the app-server schema explicitly defines as
*"Override ... for this turn and subsequent turns"*).

### Ground-Truth Verification via Rollout JSONL

Never assume policy took effect solely from `thread/resume` output. Verify from
the live `turn_context` record written to the thread's rollout file:

```bash
python3 scripts/codex_app_ctl.py status <THREAD_UUID> --verify-policy --pretty
# or directly:
python3 scripts/codex_app_ctl.py inspect-rollout --thread <THREAD_UUID> --pretty
```

Confirm `last_turn_context` matches:
- `approval_policy == "on-request"`
- `approvals_reviewer == "auto_review"`
- `sandbox_policy.type == "workspace-write"` (or `"read-only"`)
- expected `model` and `effort`

### Compressed Rollouts

Codex compresses older rollouts in place to `<rollout>.jsonl.zst`, while
`thread/read` and `thread/list` keep reporting the `.jsonl` path. When that path
is missing and `<path>.zst` exists, `codex_app_ctl.py` streams the compressed
copy through the `zstd -dc` command-line tool (Python stays dependency-free).
This covers `inspect-rollout`, `caption`, `status --verify-policy`, the
`request_user_input` lookup, and the attention fallback that `status`, `watch`,
and `retire` use when a turn summary lacks the agent message.
`inspect-rollout` reports `compressed: true` for such a file. If `zstd` is not
installed, or it cannot decompress the file, the command fails with a
`RolloutReadError` naming the file instead of reporting the rollout as missing.

## 5. Handling `turn/steer` Race Conditions

Because `turn/steer` enforces `expectedTurnId` as a precondition, a thread can
transition from `inProgress` to `completed` in the milliseconds between
`thread/turns/list` and `turn/steer`.

`codex_app_ctl.py dispatch` handles this automatically:
1. If `turn/steer` returns a turn-mismatch / no-active-turn RPC error (and
   `--steer-only` was not requested), it re-queries `latest_turn(tid)` once.
2. If the turn is no longer `inProgress`, it falls through cleanly to
   `turn/start` so the directive is never dropped.

## 6. Troubleshooting

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| `socket not found` / `daemon status != 'running'` | Local app-server daemon stopped | Run `codex app-server daemon start`, then `codex_app_ctl.py health`. |
| Exit code `3` (`quota_exhausted`) | `ordinaryUsageAllowed == false` or rate-limit reached | Check `resetsAt` from `health`; pause or trigger supervisor failover if authorized. |
| `set-policy` response still shows old reviewer | Thread is currently loaded in daemon | Next `dispatch` (`turn/start`) applies the per-turn override; verify via `inspect-rollout`. |
| Directives sit queued and unexecuted | Legacy `codex queue` used while executor stuck in a long turn | Switch to `codex_app_ctl.py dispatch` (triggers `turn/steer`) and enforce Turn-Yielding in prompts. |
| `RPC thread/resume failed: session <id> is archived` | Thread was archived and an older helper (or raw RPC) tried to resume it | Current `dispatch` unarchives automatically; otherwise run `codex_app_ctl.py unarchive <id>` and retry. |
| `RolloutReadError: Rollout ....jsonl.zst is zstd-compressed and the zstd command is not on PATH` | Codex compressed the thread's rollout and the `zstd` CLI is missing | Install `zstd` (`apt install zstd` / `brew install zstd`) and retry. |
| Existing thread stuck on old model in UI | `codex queue` inherits thread model; or thread is mid-turn | Wait for idle (or `interrupt`) and `dispatch --model <target> --effort <level>`. |
