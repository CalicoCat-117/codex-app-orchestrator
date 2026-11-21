# Supervisor Directive — Round <N> (<Slot Name>)

- **Issued:** YYYY-MM-DD HH:MM UTC
- **Target Slot / Thread:** Slot `<K>` (`<THREAD_UUID>`)
- **Worktree / Branch:** `/abs/path/to/worktree` (`<branch>`)
- **Exclusive Output Directory:** `/abs/path/to/output/<slot>/`

## 1. Strict Workspace & Safety Boundaries

1. Edit code **only** inside `/abs/path/to/worktree` on branch `<branch>`.
2. Write reports, manifests, and figures **only** inside `/abs/path/to/output/<slot>/`.
3. Do **not** edit sibling worktrees, immutable baseline artifacts, or shared boards.
4. Respect all project data/split/resource invariants (e.g., never touch held-out test sets; respect telemetry spacing >= 10s).

## 2. Verified Context (Read First)

- Prior commit / baseline receipt: `/abs/path/to/verified-receipt.json`
- Key empirical finding motivating this round: `<1–3 concise factual bullets>`

## 3. Required Deliverables

1. **Implementation:** `<specific module/script path and behavioral contract>`
2. **Verification:** `<exact lint / compile / unit test / dry-run gate to pass>`
3. **Commit & Handoff Note:** Commit cleanly on `<branch>` and write
   `/abs/path/to/output/<slot>/HANDOFF.md` with:
   - Exact commit SHA and changed file list
   - Verification commands run and exact pass/fail results
   - Integration / cherry-pick notes for Slot 1

## 4. Turn-Yielding Protocol (Mandatory)

- Do **not** poll or `sleep()` inside this turn waiting on long-running jobs (> 2 min).
- Once deliverables are committed and any async jobs/timers are recorded, report
  exact commit SHAs and artifact paths and **immediately end your turn** so the
  thread returns to `idle`.
