# Multi-Session Fleet & Periodic Supervision Patterns

This playbook distills the battle-tested orchestration patterns for running a
supervisor agent (e.g., Antigravity or Claude Code) over 1–5 persistent Codex
execution sessions on long-horizon engineering or research tasks.

## 1. Division of Responsibilities

| Role | Owner | Responsibilities |
| --- | --- | --- |
| **Supervisor / Director** | Coordinating agent | Research strategy, literature/design review, hypothesis prioritization, cross-slot integration planning, artifact & metric verification, reporting/visualization, central board updates, quota failover. |
| **Slot 1: Primary Ops / Integration Executor** | Codex session #1 | Owns main integration branch/worktree, merges/cherry-picks validated work from sibling slots, owns shared cluster/job launches and shared operational logs. |
| **Slots 2..(N-1): Isolated Feature / Experiment Writers** | Codex sessions #2..(N-1) | Each owns one dedicated `git worktree` + unique branch + exclusive `output_dir`. Zero shared-state edits, zero concurrent cluster launches unless explicitly budgeted. |
| **Slot N: Read-Only Analyst / Auditor** | Codex session #N | `sandbox: "read-only"` (or workspace-write strictly limited to its own report `output_dir`). Mines verified artifacts, performs error analysis, audits sibling branches, drafts figures/tables. |

Keep fleet size bounded to what produces genuine parallel leverage (typically
1–5 sessions). Start with 1 session for linear tasks; expand to a parallel fleet
when independent design, implementation, and diagnostic tracks can overlap.

## 2. Worktree & Directory Isolation

Before calling `launch-batch`, provision isolated git worktrees so parallel
Codex writers can never collide on index locks, branches, or untracked files:

```bash
REPO=/abs/path/to/repo
BASE_REF=main   # or your integration branch

for lane in feat-a feat-b; do
  git -C "$REPO" worktree add -b "exp/${lane}" "/abs/path/to/worktrees/${lane}" "$BASE_REF"
  mkdir -p "/abs/path/to/outputs/${lane}"
done
```

Hard boundaries to include in every worker's prompt:
- Edit **only** inside its assigned `worktree` and `output_dir`.
- Never touch sibling worktrees or shared boards/changelogs (`BOARD.md`,
  central `RESEARCH-LOG.md`, shared reservation locks).
- Never use bare `git stash` across shared worktrees (the stash stack is
  shared across all worktrees of a repo); make clean WIP commits on the slot's
  own branch instead.
- If a host has memory limits, state explicitly which checks run locally
  (e.g., `py_compile`, `ruff`, lightweight unit tests) vs on remote workers.

`codex_app_ctl.py launch-batch` validates the spec before any RPC call. It
rejects two writer slots with the same write root (the `worktree`, or `cwd` when
there is no worktree), a `branch` claimed twice, equal or nested `output_dir`s,
repeated slot numbers, and more than one `cluster_write_owner`. Nested writer
worktrees, writers without a worktree, and `danger-full-access` / `never`
policies are allowed but reported as warnings. A successful launch writes a
`FLEET-REGISTRY.json` manifest (including `validation_warnings`) used by
subsequent `status` checks.

## 3. The Turn-Yielding Protocol

A common failure mode in agent-to-agent orchestration is an executor spending an
entire turn in a `sleep()` loop waiting for a long build, training run, or CI
job. While that turn stays `inProgress`, queued instructions pile up and tokens
burn on polling.

Include this boilerplate in every executor's initial brief:

```markdown
### Turn-Yielding Protocol (Mandatory)
- Do NOT poll or `sleep()` inside an active turn waiting for long-running remote
  jobs, builds, or timers (> 2 minutes).
- Once you have implemented, verified, committed, and launched any background
  or remote job (plus configured any passive observer/timer), write a concise
  summary of state, artifact paths, and commit SHAs, then **immediately end your
  turn** so the thread returns to `idle`.
- If the supervisor injects a mid-turn correction via `turn/steer`, acknowledge
  it immediately and pivot before continuing.
```

### Questions without stalls

An executor that ends a turn on a question leaves its thread idle until
someone answers. `create` / `launch-batch` attach an autonomy guard by default
(see SKILL.md rule 6); if you write your own brief instead, keep its substance:
decide reversible choices and record them, stop only before irreversible,
out-of-scope, unauthorized, or goal-changing steps, finish independent work
first, and ask through exactly one block:

```markdown
[DECISION-NEEDED]
Question: <one sentence>
Options: A) ... | B) ...
Recommended: <option> - <one-sentence reason>
```

The supervisor finds these with `status --attention-only` (or is woken by
`watch`) and replies with `dispatch`. A turn blocked on `request_user_input`
is interrupted and restarted with the answer by `dispatch` itself.

## 4. Periodic Supervision Loop

When running continuous autonomous cycles:

1. **Schedule milestone-aligned wakeups** (e.g., 20–45 minutes, or aligned with
   an expected checkpoint/CI completion) using the supervisor's scheduler tool.
2. **Local-first audit**:
   - Run `codex_app_ctl.py health` (check quota) and
     `codex_app_ctl.py status --registry FLEET-REGISTRY.json --verify-policy --last-message`.
   - Inspect local git logs (`git -C <wt> log -n 3 --oneline`), emitted JSON
     receipts, manifests, and markdown summaries.
   - Claim completion **only** from emitted verified artifacts/receipts — never
     solely from a thread returning `idle` or a scheduler reporting `SUCCEEDED`.
   - Answer or escalate every thread that `status` marks with `attention`.
   - Between check-ins, run `codex_app_ctl.py watch --registry FLEET-REGISTRY.json`
     in the background so a finished turn or new question wakes you at once.
3. **Telemetry hygiene**:
   - Never poll shared GPUs, clusters, or control planes in tight loops.
   - Space any live remote telemetry checks by **>= 10 seconds minimum** (prefer
     15–60s+ or passive single-collector timers, with exponential backoff on
     timeouts or full pools).
4. **Integrate & advance**:
   - Have Slot 1 cherry-pick or merge verified commits from sibling worktrees
     once their unit checks and design docs pass supervisor review.
   - Write compact, self-contained round directives (see
     [examples/directive.example.md](../examples/directive.example.md)) and send
     via `codex_app_ctl.py dispatch --thread <ID> --message-file <PATH> --check-quota`.
   - Archive every slot that gets no next-round work:
     `codex_app_ctl.py retire <ID>...` (or `--registry FLEET-REGISTRY.json` when
     the run ends). It skips threads that still have a running turn, a pending
     question or decision, or a failed/interrupted last turn, so a sweep over the
     whole registry is safe; read the `skipped` reasons and handle those threads.
     `dispatch` unarchives a retired thread on demand if it needs another round.
5. **Log concisely**:
   - Append one timestamped cycle entry to your central supervision log and keep
     the user updated at meaningful milestones (or on blockers/decisions).

## 5. Quota Exhaustion Failover & Handoff-Back

Long-running multi-session fleets can occasionally hit Codex usage or rate
limits. Handle this cleanly without losing momentum or corrupting state:

### Detection
- Proactive: `codex_app_ctl.py health` shows `quota.ordinaryUsageAllowed == false`
  or non-null `rateLimitReachedType`, or `dispatch --check-quota` exits `3`.
- Reactive: `status` shows `last_turn_error` or `inspect-rollout` shows a usage
  or rate-limit error event.

### Failover (Only When User Authorized)
1. Stop dispatching new turns to Codex threads until `resetsAt` passes.
2. Record the takeover timestamp, reset time, and exact `HEAD` commit SHA in the
   central log/board.
3. Take over execution directly on the integration worktree/branch, preserving
   all project invariants, locks, and artifact naming conventions.

### Handoff-Back Once Quota Recovers
1. Verify recovery with `codex_app_ctl.py health` (`ordinaryUsageAllowed == true`).
2. Ensure the worktree is clean (all supervisor changes committed with clear
   messages) and write a structured `HANDOFF-TO-CODEX-<DATE>.md` listing:
   - Exact branch name and `HEAD` commit SHA
   - Currently running jobs/processes and their IDs/manifests
   - Verified artifacts produced during failover (paths & hashes)
   - Immutable baseline paths not to overwrite
   - Prioritized next steps and explicit boundaries
3. Dispatch the handoff packet to Slot 1 via `codex_app_ctl.py dispatch` and
   verify via `status --verify-policy` that Codex resumed cleanly before
   switching back to periodic supervisor mode.

## 6. Plain, Principle-First Session Captions

Long supervision loops degrade quickly when session outputs turn into either
raw JSON blobs, emoji-heavy hype cards, or repetitive gatekeeping checklists.

- **Worker turns:** `create` and `launch-batch` attach a brief `Caption:` guide
  in `developerInstructions` by default (`--no-caption-guide` /
  `"caption_guide": false` opts out), asking workers to conclude substantive
  turns with 3–5 plain sentences or tight bullets explaining the underlying
  mechanism/rationale and concrete observed results.
- **Supervisor reporting:** Use `codex_app_ctl.py caption --registry FLEET-REGISTRY.json`
  (or `caption <THREAD_UUID>` / offline `caption --path <rollout.jsonl>`) to
  extract and present readable captions without noisy policy dumps. Policy
  details stay hidden unless an actual error occurs or `--show-policy` is passed.
