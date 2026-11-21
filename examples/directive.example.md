# Supervisor Directive — Round <N> (<Slot Name>)

- **Issued:** YYYY-MM-DD HH:MM UTC
- **Target Slot / Thread:** Slot `<K>` (`<THREAD_UUID>`)
- **Worktree / Branch:** `/abs/path/to/worktree` (`<branch>`)
- **Exclusive Output Directory:** `/abs/path/to/output/<slot>/`

## 1. Workspace Scope

1. Edit code inside `/abs/path/to/worktree` on branch `<branch>`.
2. Write reports, manifests, and figures inside `/abs/path/to/output/<slot>/`.
3. Leave sibling worktrees, baseline artifacts, and shared boards untouched.
4. Keep project data/split invariants intact and space external telemetry checks by at least 10s.

## 2. Verified Context (Read First)

- Prior commit / baseline receipt: `/abs/path/to/verified-receipt.json`
- Key empirical finding motivating this round: `<1–3 concise factual bullets>`

## 3. Deliverables

1. **Implementation:** `<specific module/script path and behavioral contract>`
2. **Verification:** `<exact lint / compile / unit test / dry-run check>`
3. **Commit & Handoff Note:** Commit cleanly on `<branch>` and write
   `/abs/path/to/output/<slot>/HANDOFF.md` with:
   - Commit SHA and changed files
   - Verification commands run and observed results
   - Integration / cherry-pick notes for Slot 1

## 4. Turn-Yielding & Turn-End Caption

- Avoid polling or `sleep()` loops inside a turn for jobs longer than ~2 min;
  once deliverables are committed and any async jobs/timers are recorded, end
  the turn so the thread returns to `idle`.
- End the turn with a plain `Caption:` section (3–5 sentences or tight bullets)
  explaining the core mechanism or design rationale, concrete observed outcomes,
  and any remaining unknowns—no decorative emojis, hype, or compliance checklists.
