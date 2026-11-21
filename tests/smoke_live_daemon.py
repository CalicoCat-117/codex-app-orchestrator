#!/usr/bin/env python3
"""Opt-in live smoke test against the local `codex app-server` daemon.

Usage:
  # Read-only health/list/status check (zero threads created or mutated):
  python3 tests/smoke_live_daemon.py

  # Full lifecycle smoke test (creates 1 temporary thread, verifies rollout
  # Guardian `auto_review` policy, tests dispatch/interrupt, then deletes the
  # temporary thread cleanly in a `finally` block):
  python3 tests/smoke_live_daemon.py --lifecycle
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import codex_app_ctl as ctl  # noqa: E402


def run_readonly_smoke(cli: ctl.CodexAppServerClient) -> dict:
    health = cli.health()
    assert health["socketPath"], "missing socketPath"
    assert (health.get("daemon") or {}).get("status") == "running", f"daemon not running: {health.get('daemon')}"
    threads = cli.list_threads(limit=5)
    sample_status = None
    if threads:
        sample_status = cli.thread_status(threads[0]["id"], verify_policy=False)
        assert sample_status["id"] == threads[0]["id"]
    return {
        "readonly_ok": True,
        "socketPath": health["socketPath"],
        "cliVersion": (health.get("daemon") or {}).get("cliVersion"),
        "defaults": health["defaults"],
        "quota": health["quota"],
        "listed_count": len(threads),
        "sample_status_type": sample_status["status_type"] if sample_status else None,
    }


def wait_for_turn_finish_or_rollout(
    cli: ctl.CodexAppServerClient,
    tid: str,
    *,
    timeout_s: float = 90.0,
) -> dict:
    deadline = time.time() + timeout_s
    last_st = None
    while time.time() < deadline:
        st = cli.thread_status(tid, verify_policy=True, include_last_message=True)
        last_st = st
        lt_status = st.get("last_turn_status")
        if lt_status and lt_status != "inProgress":
            return st
        time.sleep(2.0)
    return last_st or {}


def run_lifecycle_smoke(cli: ctl.CodexAppServerClient, *, timeout_s: float = 90.0) -> dict:
    health = cli.health()
    if health["quota"].get("ordinaryUsageAllowed") is False:
        raise RuntimeError(f"Cannot run live lifecycle smoke test: quota exhausted ({health['quota']})")

    tid = None
    with tempfile.TemporaryDirectory(prefix="codex-app-orch-smoke-") as tmpdir:
        try:
            # 1. Create a persistent UI-visible thread with Guardian auto_review
            created = cli.start_thread(
                name="[smoke-test] codex-app-orchestrator",
                cwd=tmpdir,
                prompt=(
                    "Smoke test turn 1. Reply with exactly the single word READY. "
                    "Do not run shell commands or create files."
                ),
                approval_policy="on-request",
                approvals_reviewer="auto_review",
                sandbox="workspace-write",
            )
            tid = created["id"]
            assert tid, "thread/start did not return thread id"
            assert created["turn_id"], "turn/start did not return turn id"

            # 2. Wait for Turn 1 completion and verify ground-truth rollout turn_context
            st1 = wait_for_turn_finish_or_rollout(cli, tid, timeout_s=timeout_s)
            ctx = st1.get("rollout_turn_context") or {}
            assert ctx.get("approval_policy") == "on-request", f"unexpected approval_policy: {ctx}"
            assert ctx.get("approvals_reviewer") == "auto_review", f"unexpected approvals_reviewer: {ctx}"
            sb = ctx.get("sandbox_policy") or {}
            assert sb.get("type") in ("workspace-write", "workspaceWrite"), f"unexpected sandbox_policy: {ctx}"

            cap1 = cli.thread_caption(tid)
            cap1_text = ctl.format_session_caption(cap1)
            assert cap1["caption"], f"expected non-empty caption after turn 1, got {cap1}"
            assert "Policy:" not in cap1_text, f"default caption should stay free of policy noise: {cap1_text}"

            # 3. Dispatch Turn 2 on idle thread -> turn/start
            d2 = cli.dispatch(
                tid,
                "Smoke test turn 2. Reply with exactly the single word DONE. Do not run commands.",
                approval_policy="on-request",
                approvals_reviewer="auto_review",
                sandbox="workspace-write",
            )
            assert d2["mode"] == "turn/start", f"expected turn/start on idle thread, got {d2}"

            # 4. Immediately test active-turn `turn/steer` (or clean fallback if ultra-fast)
            d_steer = cli.dispatch(
                tid,
                "Acknowledge steering and end turn with DONE.",
                approval_policy="on-request",
                approvals_reviewer="auto_review",
                sandbox="workspace-write",
            )
            assert d_steer["mode"] in ("turn/steer", "turn/start"), f"unexpected steer result: {d_steer}"

            # 5. Test graceful interrupt if still active, else confirm clean completion
            st_mid = cli.thread_status(tid)
            if st_mid.get("last_turn_status") == "inProgress":
                inter = cli.interrupt_turn(tid)
                assert inter["interrupted"] is True
                time.sleep(1.0)
            else:
                inter = {"interrupted": False, "reason": "already_completed"}

            return {
                "lifecycle_ok": True,
                "temp_thread_id": tid,
                "turn1_status": st1.get("last_turn_status"),
                "turn1_caption_source": cap1["caption_source"],
                "turn1_caption": cap1["caption"],
                "verified_turn_context": ctx,
                "turn2_dispatch_mode": d2["mode"],
                "steer_dispatch_mode": d_steer["mode"],
                "interrupt_result": inter,
            }
        finally:
            if tid:
                try:
                    # Ensure no active turn before deleting
                    st_end = cli.thread_status(tid)
                    if st_end.get("last_turn_status") == "inProgress":
                        cli.interrupt_turn(tid)
                        time.sleep(0.5)
                except Exception:
                    pass
                del_res = cli.delete_thread(tid)
                assert del_res.get("deleted") is True
                remaining = [t["id"] for t in cli.list_threads(search="[smoke-test] codex-app-orchestrator", limit=10)]
                assert tid not in remaining, f"temp thread {tid} still visible after delete!"


def main() -> int:
    ap = argparse.ArgumentParser(description="Live smoke test for codex_app_ctl.py")
    ap.add_argument("--socket", default=None, help="Optional explicit daemon Unix socket.")
    ap.add_argument("--lifecycle", action="store_true", help="Run full create/verify/steer/delete lifecycle test.")
    ap.add_argument("--timeout", type=float, default=90.0, help="Turn wait timeout in seconds (default: 90).")
    args = ap.parse_args()

    with ctl.CodexAppServerClient(sock_path=args.socket) as cli:
        report = {"readonly": run_readonly_smoke(cli)}
        if args.lifecycle:
            report["lifecycle"] = run_lifecycle_smoke(cli, timeout_s=args.timeout)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
