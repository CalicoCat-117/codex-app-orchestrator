# Codex App Orchestrator

A portable agent skill and zero-dependency Python controller for orchestrating
one or more **persistent, UI-visible Codex execution sessions** through the
official local `codex app-server` WebSocket JSON-RPC daemon.

Designed for supervisor-executor workflows where a coordinating agent (such as
Antigravity or Claude Code) owns strategy, literature/design review,
verification, and synthesis while delegating concrete coding, debugging, and
iterative experimentation to 1–5 live Codex sessions that the user can watch
in real time in Codex Desktop or the IDE extension.

## Why This Pattern?

| Naive Approach | Failure Mode | How `codex-app-orchestrator` Solves It |
| --- | --- | --- |
| `codex exec` / `codex exec resume` | Runs headlessly — user cannot see live progress in Codex UI. | Creates and drives persistent (`ephemeral: false`) threads via `codex app-server`, visible immediately in the Codex sidebar. |
| `codex queue` while turn is active | Commands silently stall in the follow-up queue until the turn finishes. | `dispatch` inspects live turn state and uses **`turn/steer`** (`expectedTurnId`) for immediate mid-turn injection, plus enforces the Turn-Yielding Protocol. |
| `never` / `danger-full-access` | Disables safety guardrails to avoid blocking on manual clicks. | Defaults to **Guardian `auto_review`** (`approvalPolicy: "on-request"`, `approvalsReviewer: "auto_review"`, `sandbox: "workspace-write"`), verified against rollout `turn_context`. |
| Uncoordinated parallel sessions | Git index locks, branch collisions, overwritten logs/boards. | `launch-batch` validates isolated `git worktree` + unique `output_dir` per writer slot and maintains a single `FLEET-REGISTRY.json`. |
| Blind dispatch on quota limits | Loops failing turns or silently drops work. | `health` and `--check-quota` query `account/rateLimits/read` proactively (exit code `3` when exhausted) for clean pause or supervisor failover. |

## Architecture

```text
Supervisor Agent (Antigravity / Claude Code / Custom Orchestrator)
  │
  │  scripts/codex_app_ctl.py  (RFC 6455 WebSocket JSON-RPC over AF_UNIX)
  ▼
Local `codex app-server` Daemon  ◄────►  Codex Desktop / IDE UI (live visibility)
  ├── Slot 1: Primary Integration & Ops  (worktree-1, branch main-exp, ops owner)
  ├── Slot 2: Feature / Experiment A     (worktree-2, branch feat-a,   isolated)
  ├── Slot 3: Feature / Experiment B     (worktree-3, branch feat-b,   isolated)
  └── Slot N: Read-Only Analyst / Audit  (no git writes, exclusive report dir)
```

## Install

### Via `npx skills` (AgentSkills ecosystem)

```bash
npx skills add CalicoCat-117/codex-app-orchestrator --skill codex-app-orchestrator --global
```

*(For a private repository, ensure GitHub authentication is configured on the machine.)*

### Manual Install — Antigravity

Copy or symlink this repository into Antigravity's global or workspace skills folder:

```bash
# Global (all workspaces)
mkdir -p ~/.gemini/config/skills
cp -r /path/to/codex-app-orchestrator ~/.gemini/config/skills/codex-app-orchestrator

# Or project-scoped
mkdir -p /path/to/project/.agents/skills
cp -r /path/to/codex-app-orchestrator /path/to/project/.agents/skills/codex-app-orchestrator
```

### Manual Install — Claude Code / Codex

```bash
# Claude Code
mkdir -p ~/.claude/skills
cp -r /path/to/codex-app-orchestrator ~/.claude/skills/codex-app-orchestrator

# Codex
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
cp -r /path/to/codex-app-orchestrator "${CODEX_HOME:-$HOME/.codex}/skills/codex-app-orchestrator"
```

## Requirements

- **Python `>= 3.9`** (standard library only — zero external packages).
- **Codex CLI** with local `app-server` daemon support (tested with `codex-cli 0.169.0` on Linux).
- Running local daemon (`codex app-server daemon start` if not already active).

## Quickstart

```bash
# 1. Verify daemon health, defaults, and quota availability
python3 scripts/codex_app_ctl.py health --pretty

# 2. List existing UI-visible sessions
python3 scripts/codex_app_ctl.py list --limit 10 --pretty

# 3. Launch a single persistent session under Guardian auto_review
python3 scripts/codex_app_ctl.py create \
  --name "Worker · Auth Refactor" \
  --cwd /abs/path/to/repo \
  --prompt "Inspect auth.py, propose minimal fix, run tests, and end your turn." \
  --check-quota --pretty

# 4. Inspect status & verify effective rollout turn_context policy
python3 scripts/codex_app_ctl.py status <THREAD_UUID> --verify-policy --last-message --pretty

# 5. Send a directive or live mid-turn steering update (never stalls in queue)
python3 scripts/codex_app_ctl.py dispatch \
  --thread <THREAD_UUID> \
  --message "Add regression test for expired tokens before committing." \
  --check-quota --pretty

# 6. Validate and launch a multi-session isolated fleet
python3 scripts/codex_app_ctl.py launch-batch --spec examples/fleet-spec.example.json --dry-run --pretty
python3 scripts/codex_app_ctl.py launch-batch \
  --spec /abs/path/to/fleet-spec.json \
  --registry-out /abs/path/to/FLEET-REGISTRY.json \
  --check-quota --pretty
```

## CLI Subcommands

| Command | Description |
| --- | --- |
| `health` | Query daemon version, socket path, configured defaults, quota (`account/rateLimits/read`), and loaded threads. |
| `list` | Search/list threads (`--search`, `--cwd`, `--limit`, `--archived`, `--loaded-only`). |
| `status` | O(1) live status + latest turn check via `thread/turns/list` (supports `--registry`, `--verify-policy`, `--last-message`). |
| `create` | Create a persistent UI-visible thread (`thread/start` + `thread/name/set` + optional Turn-1 `turn/start`). |
| `launch-batch` | Validate isolation constraints (`--dry-run` supported), launch/reuse a fleet from JSON spec, and emit `FLEET-REGISTRY.json`. |
| `dispatch` | Smart non-blocking send: `turn/steer` if `inProgress`, `thread/resume` + `turn/start` if `notLoaded`, `turn/start` if `idle`. |
| `set-policy` | Apply `approvalPolicy`, `approvalsReviewer`, `sandbox`, and optional `model`/`effort` via `thread/resume`. |
| `interrupt` | Gracefully stop an active turn via `turn/interrupt` (auto-detects active turn ID). |
| `inspect-rollout` | Read-only inspection of rollout JSONL for ground-truth `turn_context`, recent messages, and errors. |
| `archive` / `unarchive` / `delete` | Thread lifecycle cleanup. |

## Repository Structure

```text
codex-app-orchestrator/
├── SKILL.md                                # Core skill runbook (progressive disclosure)
├── scripts/
│   └── codex_app_ctl.py                    # Zero-dependency WebSocket JSON-RPC controller
├── references/
│   ├── protocol-and-policy.md              # Wire protocol, RPC schemas, Guardian auto_review, troubleshooting
│   └── fleet-and-supervision-patterns.md   # Worktree isolation, Turn-Yielding, supervision cadence, failover
├── examples/
│   ├── fleet-spec.example.json             # Multi-session launch specification template
│   └── directive.example.md                # Structured round directive template
├── tests/
│   ├── test_codex_app_ctl.py               # Unit & mock RFC 6455 Unix-socket server test suite
│   └── smoke_live_daemon.py                # Opt-in live daemon smoke test (read-only or self-cleaning lifecycle)
├── LICENSE
└── README.md
```

## Testing

```bash
# 1. Offline unit + mock RFC 6455 Unix-socket server tests (safe anywhere, ~15 MB RAM, <1 s)
python3 -m unittest discover -s tests -v

# 2. Read-only live daemon smoke test (zero threads created or mutated)
python3 tests/smoke_live_daemon.py

# 3. Full live lifecycle smoke test (creates 1 temp thread, verifies rollout policy, tests dispatch/interrupt, deletes temp thread)
python3 tests/smoke_live_daemon.py --lifecycle
```

## License

[MIT](./LICENSE)
