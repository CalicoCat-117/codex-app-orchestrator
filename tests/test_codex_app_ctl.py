#!/usr/bin/env python3
"""Unit and mock-server integration tests for codex_app_ctl.py and SKILL.md."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import codex_app_ctl as ctl  # noqa: E402

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class MockCodexAppServer:
    """Minimal RFC 6455 WebSocket-over-Unix mock of the Codex app-server daemon."""

    def __init__(self, sock_path: Path) -> None:
        self.sock_path = sock_path
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(str(sock_path))
        self.sock.listen(8)
        self.sock.settimeout(0.2)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

        # Stateful mock data
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.config: dict[str, Any] = {
            "model": "gpt-test-model",
            "model_reasoning_effort": "max",
            "approval_policy": "on-request",
            "approvals_reviewer": "auto_review",
            "sandbox_mode": "workspace-write",
        }
        self.rate_limits: dict[str, Any] = {
            "ordinaryUsageAllowed": True,
            "rateLimits": {
                "limitId": "codex",
                "primary": {"usedPercent": 15, "windowDurationMins": 10080, "resetsAt": 1900000000},
                "rateLimitReachedType": None,
                "planType": "pro",
            },
        }
        self.threads: dict[str, dict[str, Any]] = {}
        self.loaded: set[str] = set()
        self.turns: dict[str, list[dict[str, Any]]] = {}
        self._seq = 1

        # Transport edge-case hooks
        self.send_ping_before_response = False
        self.fragment_responses = False
        self.send_unsolicited_notification = True
        self.fail_steer_race_once = False
        self.disable_turns_list = False
        self.reject_state_db_only = False
        self.received_pongs: list[bytes] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)
        try:
            self.sock.close()
        except Exception:
            pass
        if self.sock_path.exists():
            self.sock_path.unlink()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
            t.start()

    @staticmethod
    def _recv_exact(conn: socket.socket, buf: bytearray, n: int) -> bytes:
        while len(buf) < n:
            chunk = conn.recv(65536)
            if not chunk:
                raise EOFError()
            buf.extend(chunk)
        out = bytes(buf[:n])
        del buf[:n]
        return out

    def _send_frame(self, conn: socket.socket, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        first = (0x80 if fin else 0x00) | (opcode & 0x0F)
        out = bytearray([first])
        n = len(payload)
        if n < 126:
            out.append(n)
        elif n < 65536:
            out.append(126)
            out.extend(struct.pack("!H", n))
        else:
            out.append(127)
            out.extend(struct.pack("!Q", n))
        out.extend(payload)
        conn.sendall(out)

    def _send_json(self, conn: socket.socket, obj: dict[str, Any]) -> None:
        data = json.dumps(obj).encode("utf-8")
        if self.fragment_responses and len(data) > 20:
            mid = len(data) // 2
            self._send_frame(conn, 0x1, data[:mid], fin=False)
            if self.send_ping_before_response:
                self._send_frame(conn, 0x9, b"mid-ping", fin=True)
            self._send_frame(conn, 0x0, data[mid:], fin=True)
        else:
            if self.send_ping_before_response:
                self._send_frame(conn, 0x9, b"hi", fin=True)
            self._send_frame(conn, 0x1, data, fin=True)

    def _handle_conn(self, conn: socket.socket) -> None:
        conn.settimeout(5.0)
        buf = bytearray()
        try:
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf.extend(chunk)
            sep = buf.index(b"\r\n\r\n")
            headers_raw = bytes(buf[:sep]).decode("ascii", errors="replace")
            del buf[: sep + 4]
            ws_key = ""
            for line in headers_raw.splitlines():
                if line.lower().startswith("sec-websocket-key:"):
                    ws_key = line.split(":", 1)[1].strip()
            accept = base64.b64encode(
                hashlib.sha1((ws_key + WS_GUID).encode("ascii")).digest()
            ).decode("ascii")
            resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            conn.sendall(resp.encode("ascii"))

            while not self._stop.is_set():
                b1, b2 = self._recv_exact(conn, buf, 2)
                opcode = b1 & 0x0F
                masked = bool(b2 & 0x80)
                length = b2 & 0x7F
                if length == 126:
                    length = struct.unpack("!H", self._recv_exact(conn, buf, 2))[0]
                elif length == 127:
                    length = struct.unpack("!Q", self._recv_exact(conn, buf, 8))[0]
                mask = self._recv_exact(conn, buf, 4) if masked else None
                payload = self._recv_exact(conn, buf, length) if length else b""
                if mask and payload:
                    payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))

                if opcode == 0x8:  # CLOSE
                    break
                if opcode == 0xA:  # PONG
                    self.received_pongs.append(payload)
                    continue
                if opcode != 0x1:
                    continue

                msg = json.loads(payload.decode("utf-8"))
                method = msg.get("method")
                params = msg.get("params") or {}
                req_id = msg.get("id")
                self.calls.append((method, params))

                if req_id is None:
                    # Notification (e.g., `initialized`)
                    continue

                if self.send_unsolicited_notification and method != "initialize":
                    self._send_json(conn, {"method": "server/heartbeat", "params": {"ok": True}})

                try:
                    result = self._dispatch_rpc(method, params)
                    self._send_json(conn, {"id": req_id, "result": result})
                except ValueError as exc:
                    self._send_json(conn, {"id": req_id, "error": {"code": -32000, "message": str(exc)}})
        except (EOFError, OSError):
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _new_id(self, prefix: str) -> str:
        val = f"{prefix}-{self._seq:04d}"
        self._seq += 1
        return val

    def _dispatch_rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "initialize":
            return {"userAgent": "MockCodex/0.169.0", "platformOs": "linux"}
        if method == "config/read":
            return {"config": dict(self.config)}
        if method == "account/rateLimits/read":
            return dict(self.rate_limits)
        if method == "thread/loaded/list":
            return {"data": sorted(self.loaded), "nextCursor": None}
        if method == "thread/list":
            if self.reject_state_db_only and "useStateDbOnly" in params:
                raise ValueError("unknown field `useStateDbOnly`")
            rows = list(self.threads.values())
            if params.get("searchTerm"):
                needle = params["searchTerm"].lower()
                rows = [r for r in rows if needle in (r.get("name") or "").lower()]
            if params.get("cwd"):
                rows = [r for r in rows if r.get("cwd") == params["cwd"]]
            if params.get("archived") is False:
                rows = [r for r in rows if not r.get("archived")]
            limit = int(params.get("limit", 25))
            return {"data": rows[:limit], "nextCursor": None}
        if method == "thread/start":
            tid = self._new_id("thr")
            model = params.get("model") or self.config["model"]
            effort = (params.get("config") or {}).get("model_reasoning_effort") or self.config["model_reasoning_effort"]
            th = {
                "id": tid,
                "name": "",
                "cwd": params.get("cwd") or "/tmp",
                "model": model,
                "reasoningEffort": effort,
                "status": {"type": "idle"},
                "source": "vscode",
                "ephemeral": bool(params.get("ephemeral", False)),
                "approvalPolicy": params.get("approvalPolicy", "on-request"),
                "approvalsReviewer": params.get("approvalsReviewer", "auto_review"),
                "sandbox": params.get("sandbox", "workspace-write"),
                "updatedAt": 1795000000,
                "path": None,
            }
            self.threads[tid] = th
            self.turns[tid] = []
            self.loaded.add(tid)
            return {
                "thread": th,
                "model": model,
                "reasoningEffort": effort,
                "approvalPolicy": th["approvalPolicy"],
                "approvalsReviewer": th["approvalsReviewer"],
                "sandbox": th["sandbox"],
            }
        if method == "thread/name/set":
            tid = params["threadId"]
            self.threads[tid]["name"] = params["name"]
            return {}
        if method == "thread/read":
            tid = params["threadId"]
            if tid not in self.threads:
                raise ValueError(f"thread not found: {tid}")
            th = dict(self.threads[tid])
            if params.get("includeTurns"):
                th["turns"] = list(self.turns.get(tid, []))
            return {"thread": th}
        if method == "thread/turns/list":
            if self.disable_turns_list:
                raise ValueError("method not supported: thread/turns/list")
            tid = params["threadId"]
            tlist = list(self.turns.get(tid, []))
            if params.get("sortDirection", "desc") == "desc":
                tlist = list(reversed(tlist))
            limit = int(params.get("limit", 10))
            return {"data": tlist[:limit], "nextCursor": None}
        if method == "thread/resume":
            tid = params["threadId"]
            th = self.threads[tid]
            was_loaded = tid in self.loaded
            self.loaded.add(tid)
            th["status"] = {"type": "idle"}
            # Replicate real daemon nuance: loaded threads keep cached policy until next turn/start.
            if not was_loaded:
                if params.get("approvalPolicy"):
                    th["approvalPolicy"] = params["approvalPolicy"]
                if params.get("approvalsReviewer"):
                    th["approvalsReviewer"] = params["approvalsReviewer"]
                if params.get("sandbox"):
                    th["sandbox"] = params["sandbox"]
                if params.get("model"):
                    th["model"] = params["model"]
            return {
                "thread": th,
                "model": th["model"],
                "reasoningEffort": th["reasoningEffort"],
                "approvalPolicy": th["approvalPolicy"],
                "approvalsReviewer": th["approvalsReviewer"],
                "sandbox": th["sandbox"],
            }
        if method == "turn/start":
            tid = params["threadId"]
            th = self.threads[tid]
            if params.get("approvalPolicy"):
                th["approvalPolicy"] = params["approvalPolicy"]
            if params.get("approvalsReviewer"):
                th["approvalsReviewer"] = params["approvalsReviewer"]
            if params.get("model"):
                th["model"] = params["model"]
            if params.get("effort"):
                th["reasoningEffort"] = params["effort"]
            turn_id = self._new_id("turn")
            turn = {
                "id": turn_id,
                "status": "inProgress",
                "error": None,
                "items": [{"type": "userMessage", "content": params.get("input", [])}],
            }
            self.turns.setdefault(tid, []).append(turn)
            th["status"] = {"type": "active"}
            return {"turn": turn}
        if method == "turn/steer":
            tid = params["threadId"]
            expected = params["expectedTurnId"]
            if self.fail_steer_race_once:
                self.fail_steer_race_once = False
                self.turns[tid][-1]["status"] = "completed"
                self.threads[tid]["status"] = {"type": "idle"}
                raise ValueError(f"expectedTurnId mismatch: turn {expected} already completed")
            tlist = self.turns.get(tid, [])
            if not tlist or tlist[-1]["id"] != expected or tlist[-1]["status"] != "inProgress":
                raise ValueError("no active turn matching expectedTurnId")
            return {"turnId": expected}
        if method == "turn/interrupt":
            tid = params["threadId"]
            turn_id = params["turnId"]
            for tr in self.turns.get(tid, []):
                if tr["id"] == turn_id:
                    tr["status"] = "interrupted"
            self.threads[tid]["status"] = {"type": "idle"}
            return {}
        if method == "thread/archive":
            self.threads[params["threadId"]]["archived"] = True
            return {}
        if method == "thread/unarchive":
            self.threads[params["threadId"]]["archived"] = False
            return {}
        if method == "thread/delete":
            tid = params["threadId"]
            self.threads.pop(tid, None)
            self.turns.pop(tid, None)
            self.loaded.discard(tid)
            return {}
        raise ValueError(f"unknown method: {method}")


class TestCodexAppCtl(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.sock_path = self.tmp_path / "mock-codex.sock"
        self.server = MockCodexAppServer(self.sock_path)
        self.server.start()

    def tearDown(self) -> None:
        self.server.stop()
        self.tmpdir.cleanup()

    def _run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "codex_app_ctl.py"), "--socket", str(self.sock_path), *args],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def test_websocket_fragmentation_ping_and_large_payload(self) -> None:
        self.server.send_ping_before_response = True
        self.server.fragment_responses = True
        big_prompt = "A" * 70000  # forces 64-bit extended payload length (>65535 bytes)
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            res = cli.start_thread(name="Large Prompt Session", cwd=str(self.tmp_path), prompt=big_prompt)
            self.assertEqual(res["name"], "Large Prompt Session")
            self.assertTrue(res["turn_id"].startswith("turn-"))
        self.assertGreaterEqual(len(self.server.received_pongs), 1)
        self.assertIn(b"mid-ping", self.server.received_pongs)

    def test_health_and_quota_gating(self) -> None:
        proc = self._run_cli("health")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        data = json.loads(proc.stdout)
        self.assertEqual(data["defaults"]["model"], "gpt-test-model")
        self.assertTrue(data["quota"]["ordinaryUsageAllowed"])

        # Simulate quota exhaustion -> exit code 3 on `--check-quota`
        self.server.rate_limits["ordinaryUsageAllowed"] = False
        self.server.rate_limits["rateLimits"]["rateLimitReachedType"] = "weekly"
        proc_q = self._run_cli(
            "create",
            "--name",
            "Blocked",
            "--cwd",
            str(self.tmp_path),
            "--check-quota",
        )
        self.assertEqual(proc_q.returncode, 3)
        err = json.loads(proc_q.stdout)
        self.assertEqual(err["error"], "quota_exhausted")

    def test_create_defaults_guardian_auto_review_and_persistent(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            res = cli.start_thread(
                name="Guardian Worker",
                cwd=str(self.tmp_path),
                prompt="Run checks",
            )
            self.assertEqual(res["approvalPolicy"], "on-request")
            self.assertEqual(res["approvalsReviewer"], "auto_review")
            self.assertEqual(res["sandbox"], "workspace-write")
            self.assertFalse(self.server.threads[res["id"]]["ephemeral"])

        start_calls = [p for m, p in self.server.calls if m == "turn/start"]
        self.assertEqual(len(start_calls), 1)
        self.assertEqual(start_calls[0]["sandboxPolicy"]["type"], "workspaceWrite")
        self.assertEqual(start_calls[0]["approvalsReviewer"], "auto_review")

    def test_dispatch_routes_steer_resume_idle_and_race_fallback(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(name="Router Test", cwd=str(self.tmp_path), prompt="Initial")
            tid = created["id"]
            first_turn = created["turn_id"]

            # 1. Active turn -> turn/steer with exact expectedTurnId
            r_steer = cli.dispatch(tid, "Mid-turn correction")
            self.assertEqual(r_steer["mode"], "turn/steer")
            self.assertEqual(r_steer["expectedTurnId"], first_turn)

            # 2. Race condition: turn finishes right as turn/steer executes -> auto-fallback to turn/start
            self.server.fail_steer_race_once = True
            r_race = cli.dispatch(tid, "Race fallback directive")
            self.assertEqual(r_race["mode"], "turn/start")
            self.assertNotEqual(r_race["turnId"], first_turn)

            # 3. Interrupt active turn -> becomes idle -> dispatch starts a fresh turn
            r_int = cli.interrupt_turn(tid)
            self.assertTrue(r_int["interrupted"])
            r_idle = cli.dispatch(tid, "Next round after idle")
            self.assertEqual(r_idle["mode"], "turn/start")

            # 4. Steer-only skips when idle
            self.server.turns[tid][-1]["status"] = "completed"
            self.server.threads[tid]["status"] = {"type": "idle"}
            r_skip = cli.dispatch(tid, "Only if active", steer_only=True)
            self.assertEqual(r_skip["mode"], "skipped")
            self.assertEqual(r_skip["reason"], "no_active_turn")

            # 5. Unloaded (`notLoaded`) -> calls thread/resume then turn/start
            self.server.loaded.discard(tid)
            self.server.threads[tid]["status"] = {"type": "notLoaded"}
            r_unloaded = cli.dispatch(tid, "Wake from notLoaded")
            self.assertEqual(r_unloaded["mode"], "turn/start")
            self.assertTrue(r_unloaded["resumedFromNotLoaded"])
            self.assertIn(tid, self.server.loaded)

    def test_turns_list_fallback_for_older_servers(self) -> None:
        self.server.disable_turns_list = True
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(name="Legacy Fallback", cwd=str(self.tmp_path), prompt="Hello")
            st = cli.thread_status(created["id"])
            self.assertEqual(st["last_turn_id"], created["turn_id"])
            self.assertEqual(st["last_turn_status"], "inProgress")

    def test_list_threads_uses_state_db_only_and_falls_back(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            cli.start_thread(name="Fast List Check", cwd=str(self.tmp_path))
            rows = cli.list_threads(search="Fast List", limit=5)
            self.assertEqual(len(rows), 1)

        list_calls = [p for m, p in self.server.calls if m == "thread/list"]
        self.assertTrue(list_calls[-1].get("useStateDbOnly"))

        # Older server rejecting `useStateDbOnly` should fall back transparently
        self.server.reject_state_db_only = True
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            rows_legacy = cli.list_threads(search="Fast List", limit=5)
            self.assertEqual(len(rows_legacy), 1)
        self.server.reject_state_db_only = False

        # `--scan-rollouts` explicitly disables `useStateDbOnly`
        proc_scan = self._run_cli("list", "--search", "Fast List", "--scan-rollouts", "--pretty")
        self.assertEqual(proc_scan.returncode, 0, proc_scan.stderr)
        list_calls_after = [p for m, p in self.server.calls if m == "thread/list"]
        self.assertNotIn("useStateDbOnly", list_calls_after[-1])

    def test_loaded_thread_set_policy_note_and_turn_override(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(
                name="Legacy Policy",
                cwd=str(self.tmp_path),
                approval_policy="never",
                approvals_reviewer="user",
                sandbox="danger-full-access",
            )
            tid = created["id"]
            # Thread is already loaded, so thread/resume keeps cached policy and warns
            pol = cli.apply_policy(tid, approval_policy="on-request", approvals_reviewer="auto_review")
            self.assertTrue(pol["wasLoaded"])
            self.assertIsNotNone(pol["note"])
            # Next dispatch passes explicit per-turn overrides in turn/start
            cli.dispatch(tid, "Apply Guardian on next turn", approval_policy="on-request", approvals_reviewer="auto_review")
            self.assertEqual(self.server.threads[tid]["approvalsReviewer"], "auto_review")

    def test_rollout_inspection_and_status_verify_policy(self) -> None:
        rollout_path = self.tmp_path / "rollout-sample.jsonl"
        entries = [
            {
                "timestamp": "2026-11-21T00:00:01Z",
                "type": "turn_context",
                "payload": {
                    "turn_id": "turn-0001",
                    "model": "gpt-test-model",
                    "effort": "max",
                    "approval_policy": "on-request",
                    "approvals_reviewer": "auto_review",
                    "sandbox_policy": {"type": "workspace-write", "network_access": False},
                    "cwd": str(self.tmp_path),
                },
            },
            {
                "timestamp": "2026-11-21T00:00:02Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "All unit tests passed."}],
                },
            },
            {
                "timestamp": "2026-11-21T00:00:03Z",
                "type": "event_msg",
                "payload": {"type": "task_complete"},
            },
        ]
        rollout_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")

        # Offline CLI path
        proc_off = self._run_cli("inspect-rollout", "--path", str(rollout_path))
        self.assertEqual(proc_off.returncode, 0, proc_off.stderr)
        parsed = json.loads(proc_off.stdout)
        self.assertEqual(parsed["last_turn_context"]["approvals_reviewer"], "auto_review")
        self.assertEqual(parsed["task_complete_count"], 1)
        self.assertEqual(parsed["recent_assistant_messages"][0]["preview"], "All unit tests passed.")

        # Integrated status --verify-policy
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(name="Policy Verify", cwd=str(self.tmp_path), prompt="Go")
            self.server.threads[created["id"]]["path"] = str(rollout_path)
            st = cli.thread_status(created["id"], verify_policy=True, include_last_message=True)
            self.assertEqual(st["rollout_turn_context"]["approval_policy"], "on-request")
            self.assertEqual(st["last_agent_message"], "All unit tests passed.")

    def test_launch_batch_isolation_validation_and_registry(self) -> None:
        # 1. Collision detection must fail fast before any RPC writes
        bad_spec = self.tmp_path / "bad-spec.json"
        bad_spec.write_text(
            json.dumps(
                [
                    {"name": "W1", "cwd": str(self.tmp_path), "worktree": str(self.tmp_path / "wt1"), "output_dir": str(self.tmp_path / "out1")},
                    {"name": "W2", "cwd": str(self.tmp_path), "worktree": str(self.tmp_path / "wt1"), "output_dir": str(self.tmp_path / "out2")},
                ]
            ),
            encoding="utf-8",
        )
        proc_bad = self._run_cli("launch-batch", "--spec", str(bad_spec), "--dry-run")
        self.assertNotEqual(proc_bad.returncode, 0)

        # 2. Valid spec with prompt_file + existing_thread_id reuse + registry output
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            existing = cli.start_thread(name="Existing Ops", cwd=str(self.tmp_path))
            existing_id = existing["id"]

        prompt_file = self.tmp_path / "slot2.md"
        prompt_file.write_text("Execute isolated feature track.", encoding="utf-8")

        good_spec = self.tmp_path / "good-spec.json"
        reg_out = self.tmp_path / "FLEET-REGISTRY.json"
        good_spec.write_text(
            json.dumps(
                [
                    {
                        "slot": 1,
                        "name": "Existing Ops",
                        "existing_thread_id": existing_id,
                        "cwd": str(self.tmp_path),
                        "worktree": str(self.tmp_path / "wt-ops"),
                        "output_dir": str(self.tmp_path / "out-ops"),
                        "cluster_write_owner": True,
                        "prompt": "Coordinate fleet.",
                    },
                    {
                        "slot": 2,
                        "name": "New Isolated Track",
                        "cwd": str(self.tmp_path),
                        "worktree": str(self.tmp_path / "wt-track2"),
                        "output_dir": str(self.tmp_path / "out-track2"),
                        "prompt_file": "slot2.md",
                    },
                ]
            ),
            encoding="utf-8",
        )
        proc_dry = self._run_cli("launch-batch", "--spec", str(good_spec), "--dry-run")
        self.assertEqual(proc_dry.returncode, 0, proc_dry.stderr)
        self.assertEqual(json.loads(proc_dry.stdout)["count"], 2)

        proc_run = self._run_cli("launch-batch", "--spec", str(good_spec), "--registry-out", str(reg_out))
        self.assertEqual(proc_run.returncode, 0, proc_run.stderr)
        self.assertTrue(reg_out.exists())
        reg = json.loads(reg_out.read_text(encoding="utf-8"))
        self.assertEqual(reg["session_count"], 2)
        self.assertEqual(reg["sessions"][0]["id"], existing_id)

        # Verify `status --registry` reads both slots cleanly
        proc_st = self._run_cli("status", "--registry", str(reg_out))
        self.assertEqual(proc_st.returncode, 0, proc_st.stderr)
        lines = [json.loads(line) for line in proc_st.stdout.strip().splitlines()]
        self.assertEqual(len(lines), 2)

    def _fleet_base(self) -> list[dict]:
        t = self.tmp_path
        return [
            {"slot": 1, "name": "Ops", "cwd": str(t), "worktree": str(t / "wt-ops"), "branch": "exp/ops",
             "output_dir": str(t / "out" / "ops"), "cluster_write_owner": True},
            {"slot": 2, "name": "Track A", "cwd": str(t), "worktree": str(t / "wt-a"), "branch": "exp/a",
             "output_dir": str(t / "out" / "a")},
            {"slot": 3, "name": "Analyst", "cwd": str(t), "read_only": True, "output_dir": str(t / "out" / "analyst")},
        ]

    def _dry_run(self, spec: list[dict], tag: str) -> subprocess.CompletedProcess[str]:
        path = self.tmp_path / f"spec-{tag}.json"
        path.write_text(json.dumps(spec), encoding="utf-8")
        return self._run_cli("launch-batch", "--spec", str(path), "--dry-run")

    def test_launch_batch_rejects_isolation_gaps_with_json_errors(self) -> None:
        t = self.tmp_path
        ok = self._dry_run(self._fleet_base(), "ok")
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        self.assertEqual(json.loads(ok.stdout)["warnings"], [])

        def mutate(fn):
            spec = self._fleet_base()
            fn(spec)
            return spec

        def no_worktree(s, i):
            s[i]["worktree"] = None
            s[i]["branch"] = None

        cases = {
            "two_writers_shared_cwd": (mutate(lambda s: (no_worktree(s, 0), no_worktree(s, 1))), "Writer isolation collision"),
            "same_worktree": (mutate(lambda s: s[1].update(worktree=s[0]["worktree"] + "/")), "Worktree isolation collision"),
            "dup_branch": (mutate(lambda s: s[1].update(branch="exp/ops")), "Branch isolation collision"),
            "two_owners": (mutate(lambda s: s[1].update(cluster_write_owner=True)), "cluster_write_owner"),
            "nested_output": (mutate(lambda s: s[1].update(output_dir=str(t / "out" / "ops" / "a"))), "are nested"),
            "same_output": (mutate(lambda s: s[2].update(output_dir=s[1]["output_dir"])), "Output directory isolation collision"),
            "dup_slot": (mutate(lambda s: s[2].update(slot=2)), "Duplicate slot 2"),
            "bad_json": (None, "JSONDecodeError"),
        }
        for tag, (spec, needle) in cases.items():
            with self.subTest(tag):
                if spec is None:
                    path = self.tmp_path / "spec-bad_json.json"
                    path.write_text("[{not json", encoding="utf-8")
                    proc = self._run_cli("launch-batch", "--spec", str(path), "--dry-run")
                else:
                    proc = self._dry_run(spec, tag)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertNotIn("Traceback", proc.stderr)
                err = json.loads(proc.stdout)
                self.assertFalse(err["ok"])
                self.assertIn(needle, err["error"] + " " + err["detail"])

        # A missing spec file is a clean JSON error too.
        missing = self._run_cli("launch-batch", "--spec", str(t / "nope.json"), "--dry-run")
        self.assertEqual(missing.returncode, 1)
        self.assertNotIn("Traceback", missing.stderr)
        self.assertEqual(json.loads(missing.stdout)["error"], "FileNotFoundError")

        # Soft findings pass but are reported.
        soft = mutate(lambda s: (
            s[1].update(worktree=str(t / "wt-ops" / ".worktrees" / "a")),
            s[1].update(sandbox="danger-full-access"),
        ))
        proc_soft = self._dry_run(soft, "soft")
        self.assertEqual(proc_soft.returncode, 0, proc_soft.stdout + proc_soft.stderr)
        warnings = json.loads(proc_soft.stdout)["warnings"]
        self.assertTrue(any("nested inside the write root of 'Ops'" in w for w in warnings), warnings)
        self.assertTrue(any("danger-full-access" in w for w in warnings), warnings)

        lone = mutate(lambda s: no_worktree(s, 1))
        proc_lone = self._dry_run(lone, "lone")
        self.assertEqual(proc_lone.returncode, 0, proc_lone.stdout + proc_lone.stderr)
        self.assertTrue(
            any("'Track A' has no dedicated worktree" in w for w in json.loads(proc_lone.stdout)["warnings"])
        )

    def test_offline_caption_reports_last_turn_state_not_any_completion(self) -> None:
        def write_rollout(name: str, events: list[dict]) -> Path:
            path = self.tmp_path / name
            path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
            return path

        def turn(tid: str, end: str | None, text: str | None = None) -> list[dict]:
            evs = [
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": tid}},
                {"type": "turn_context", "payload": {"turn_id": tid, "model": "m", "effort": "low", "cwd": "/w"}},
            ]
            if text:
                evs.append({"type": "response_item", "payload": {
                    "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}})
            if end == "complete":
                evs.append({"type": "event_msg", "payload": {"type": "task_complete", "turn_id": tid}})
            elif end:
                evs.append({"type": "event_msg", "payload": {"type": "turn_aborted", "turn_id": tid, "reason": end}})
            return evs

        def offline_caption(path: Path) -> tuple[dict, str]:
            rec = json.loads(self._run_cli("caption", "--path", str(path), "--json").stdout)
            plain = self._run_cli("caption", "--path", str(path)).stdout
            return rec, plain

        # Completed turn followed by an interrupted one: must not claim "completed".
        p1 = write_rollout("r1.jsonl", turn("t1", "complete", "First turn done.") + turn("t2", "interrupted"))
        rec, plain = offline_caption(p1)
        self.assertEqual(rec["status_type"], "offline")
        self.assertEqual(rec["last_turn_id"], "t2")
        self.assertEqual(rec["last_turn_status"], "interrupted")
        self.assertIn("last turn interrupted", plain)
        self.assertIn("turn_aborted: interrupted", plain)
        inspected = ctl.inspect_rollout_file(p1)
        self.assertEqual(inspected["recent_errors"][-1]["turn_id"], "t2")

        # Interrupted turn followed by a completed one: completed, and no stale abort note.
        p2 = write_rollout("r2.jsonl", turn("t1", "interrupted") + turn("t2", "complete", "Recovered."))
        rec2, plain2 = offline_caption(p2)
        self.assertEqual(rec2["last_turn_status"], "completed")
        self.assertIsNone(rec2["error"])
        self.assertNotIn("Note:", plain2)

        # Still running, and a non-interrupt abort reason.
        p3 = write_rollout("r3.jsonl", turn("t1", "complete", "ok") + turn("t2", None))
        self.assertEqual(offline_caption(p3)[0]["last_turn_status"], "inProgress")
        p4 = write_rollout("r4.jsonl", turn("t1", "replaced"))
        self.assertEqual(offline_caption(p4)[0]["last_turn_status"], "aborted")

        # Old-format rollout with no turn ids anywhere: second context opens a new turn.
        old = [
            {"type": "turn_context", "payload": {"model": "m"}},
            {"type": "event_msg", "payload": {"type": "task_complete"}},
            {"type": "turn_context", "payload": {"model": "m"}},
        ]
        self.assertEqual(ctl.inspect_rollout_file(write_rollout("r5.jsonl", old))["last_turn_status"], "inProgress")

    def test_global_flags_before_after_and_mixed_with_subcommands(self) -> None:
        # 1. Trailing global flags after subcommand (matches SKILL.md / README.md examples)
        proc_after = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "codex_app_ctl.py"),
                "health",
                "--socket",
                str(self.sock_path),
                "--timeout",
                "12",
                "--pretty",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(proc_after.returncode, 0, proc_after.stderr)
        self.assertIn('\n  "socketPath":', proc_after.stdout)
        self.assertEqual(json.loads(proc_after.stdout)["defaults"]["model"], "gpt-test-model")

        # 2. Leading global flags before subcommand must not be clobbered by subparser defaults
        proc_before = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "codex_app_ctl.py"),
                "--pretty",
                "--timeout",
                "12",
                "--socket",
                str(self.sock_path),
                "health",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(proc_before.returncode, 0, proc_before.stderr)
        self.assertIn('\n  "socketPath":', proc_before.stdout)

        # 3. Mixed leading + trailing global flags across offline and RPC commands
        parser = ctl.build_parser()
        ns = parser.parse_args(["--timeout", "7.5", "list", "--limit", "3", "--pretty", "--socket", "/tmp/demo.sock"])
        self.assertEqual(ns.cmd, "list")
        self.assertEqual(ns.timeout, 7.5)
        self.assertEqual(ns.limit, 3)
        self.assertTrue(ns.pretty)
        self.assertEqual(ns.socket, "/tmp/demo.sock")

        example_spec = REPO_ROOT / "examples" / "fleet-spec.example.json"
        proc_dry = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "codex_app_ctl.py"),
                "launch-batch",
                "--spec",
                str(example_spec),
                "--dry-run",
                "--pretty",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(proc_dry.returncode, 0, proc_dry.stderr)
        self.assertIn('\n  "dry_run": true', proc_dry.stdout)

    def test_caption_guide_composition_and_opt_out(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            cli.start_thread(name="Default Caption Guide", cwd=str(self.tmp_path))
            cli.start_thread(
                name="Composed Guide",
                cwd=str(self.tmp_path),
                developer_instructions="Focus on numerical stability.",
            )
            cli.start_thread(
                name="Opt-Out Guide",
                cwd=str(self.tmp_path),
                caption_guide=False,
            )

        start_calls = [p for m, p in self.server.calls if m == "thread/start"]
        self.assertIn("Caption:", start_calls[0]["developerInstructions"])
        self.assertIn("Focus on numerical stability.", start_calls[1]["developerInstructions"])
        self.assertIn("Caption:", start_calls[1]["developerInstructions"])
        self.assertNotIn("developerInstructions", start_calls[2])

        # CLI `--no-caption-guide` flag disables injection
        proc = self._run_cli("create", "--name", "CLI Opt-Out", "--cwd", str(self.tmp_path), "--no-caption-guide")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        last_start = [p for m, p in self.server.calls if m == "thread/start"][-1]
        self.assertNotIn("developerInstructions", last_start)

    def test_caption_extraction_and_cli_rendering(self) -> None:
        # 1. Explicit Caption section extraction + emoji cleanup + stop at next heading
        raw = (
            "Detailed build log...\n\n"
            "## Caption\n"
            "- ✅ Querying the state DB avoids scanning hundreds of rollout JSONLs.\n"
            "- Latency dropped from >60s timeout to 12ms across 500 sessions.\n\n"
            "## Appendix\n"
            "Unrelated raw dump."
        )
        cap, src = ctl.extract_caption_text(raw)
        self.assertEqual(src, "explicit_caption")
        self.assertNotIn("✅", cap)
        self.assertNotIn("Appendix", cap)
        self.assertIn("Querying the state DB", cap)
        self.assertIn("12ms", cap)

        # Inline bold / CJK colon form
        cap_inline, src_inline = ctl.extract_caption_text("前置分析...\n\n**Caption：** 采用增量状态库查询以消除全盘扫描阻塞。")
        self.assertEqual(src_inline, "explicit_caption")
        self.assertEqual(cap_inline, "采用增量状态库查询以消除全盘扫描阻塞。")

        # Fallback when no explicit Caption section exists, with boundary trim
        cap_fb, src_fb = ctl.extract_caption_text("First sentence explains mechanism. Second sentence adds detail.", max_chars=45)
        self.assertEqual(src_fb, "latest_message")
        self.assertTrue(cap_fb.endswith("..."))
        self.assertIn("First sentence explains mechanism.", cap_fb)

        # 2. Offline rollout caption CLI (plain text omits noisy policy unless `--show-policy`)
        rollout_path = self.tmp_path / "rollout-caption.jsonl"
        entries = [
            {
                "timestamp": "2026-11-21T00:00:01Z",
                "type": "turn_context",
                "payload": {
                    "turn_id": "turn-0009",
                    "model": "gpt-test-model",
                    "effort": "max",
                    "approval_policy": "on-request",
                    "approvals_reviewer": "auto_review",
                    "sandbox_policy": {"type": "workspace-write"},
                    "cwd": str(self.tmp_path),
                },
            },
            {
                "timestamp": "2026-11-21T00:00:02Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": (
                                "Long trace omitted.\n\n"
                                "Caption:\n"
                                "Replaced blocking JSONL scans with state-DB queries because filtered "
                                "thread/list calls previously re-read every historical rollout on disk. "
                                "All 12 unit tests pass."
                            ),
                        }
                    ],
                },
            },
            {
                "timestamp": "2026-11-21T00:00:03Z",
                "type": "event_msg",
                "payload": {"type": "task_complete"},
            },
        ]
        rollout_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")

        proc_plain = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "codex_app_ctl.py"), "caption", "--path", str(rollout_path)],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(proc_plain.returncode, 0, proc_plain.stderr)
        self.assertIn("Caption:", proc_plain.stdout)
        self.assertIn("Replaced blocking JSONL scans", proc_plain.stdout)
        self.assertNotIn("Policy:", proc_plain.stdout)

        proc_pol = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "codex_app_ctl.py"),
                "caption",
                "--path",
                str(rollout_path),
                "--show-policy",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(proc_pol.returncode, 0, proc_pol.stderr)
        self.assertIn("Policy: on-request / auto_review / workspace-write", proc_pol.stdout)

        # 3. Live mock-server `caption --registry` + `--json --pretty` + active turn
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            th1 = cli.start_thread(name="Search Fix", cwd=str(self.tmp_path), prompt="Fix search latency")
            tid1 = th1["id"]
            self.server.threads[tid1]["path"] = str(rollout_path)
            self.server.threads[tid1]["status"] = {"type": "idle"}
            self.server.turns[tid1][-1]["status"] = "completed"

            th2 = cli.start_thread(name="Active Worker", cwd=str(self.tmp_path), prompt="Measure memory overhead")
            tid2 = th2["id"]

        reg_path = self.tmp_path / "FLEET-REGISTRY.json"
        reg_path.write_text(
            json.dumps(
                {
                    "sessions": [
                        {"slot": 1, "id": tid1, "name": "Search Fix", "branch": "exp/fast-list"},
                        {"slot": 2, "id": tid2, "name": "Active Worker", "branch": "exp/mem"},
                    ]
                }
            ),
            encoding="utf-8",
        )

        proc_reg = self._run_cli("caption", "--registry", str(reg_path))
        self.assertEqual(proc_reg.returncode, 0, proc_reg.stderr)
        self.assertIn("Slot 1 · Search Fix — idle (last turn completed)", proc_reg.stdout)
        self.assertIn("(exp/fast-list)", proc_reg.stdout)
        self.assertIn("Replaced blocking JSONL scans", proc_reg.stdout)
        self.assertIn("Slot 2 · Active Worker — active (turn in progress)", proc_reg.stdout)
        self.assertIn("Measure memory overhead", proc_reg.stdout)

        proc_json = self._run_cli("caption", tid1, "--json", "--pretty")
        self.assertEqual(proc_json.returncode, 0, proc_json.stderr)
        parsed = json.loads(proc_json.stdout)
        self.assertEqual(parsed["caption_source"], "explicit_caption")
        self.assertIn("Replaced blocking JSONL scans", parsed["caption"])

    def test_archive_unarchive_and_delete(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(name="Ephemeral Cleanup", cwd=str(self.tmp_path))
            tid = created["id"]
            self.assertTrue(cli.archive_thread(tid)["archived"])
            self.assertTrue(self.server.threads[tid]["archived"])
            self.assertTrue(cli.unarchive_thread(tid)["unarchived"])
            self.assertFalse(self.server.threads[tid]["archived"])
            self.assertTrue(cli.delete_thread(tid)["deleted"])
            self.assertNotIn(tid, self.server.threads)

    def test_skill_markdown_frontmatter_and_relative_links(self) -> None:
        skill_md = REPO_ROOT / "SKILL.md"
        text = skill_md.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("---\n"))
        end = text.index("\n---\n", 4)
        fm = text[4:end]
        self.assertIn("name: codex-app-orchestrator", fm)
        self.assertIn("description:", fm)
        # Verify all relative markdown links in SKILL.md point to existing files
        for rel in re.findall(r"\]\(\./([^)#]+)\)", text):
            target = REPO_ROOT / rel
            self.assertTrue(target.exists(), f"Broken SKILL.md link: ./{rel}")
        # Ensure example fleet spec passes dry-run validation
        example_spec = REPO_ROOT / "examples" / "fleet-spec.example.json"
        validated = ctl.validate_fleet_spec(
            json.loads(example_spec.read_text(encoding="utf-8")),
            example_spec.parent,
        )
        self.assertEqual(len(validated), 4)


if __name__ == "__main__":
    unittest.main()
