#!/usr/bin/env python3
"""Unit and mock-server integration tests for codex_app_ctl.py and SKILL.md."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
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
        self.models: list[dict[str, Any]] | None = [
            {"id": "gpt-test-model", "supportedReasoningEfforts": [{"reasoningEffort": "max"}]},
        ]
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
        self.thread_list_page_max = 100
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
        if method == "model/list":
            if self.models is None:
                raise ValueError("model/list not supported")
            # Two pages, to exercise cursor handling.
            if params.get("cursor") == "page2":
                return {"data": self.models[1:], "nextCursor": None}
            return {"data": self.models[:1], "nextCursor": "page2" if len(self.models) > 1 else None}
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
            # Like the real server: pages are capped, and `nextCursor` continues the listing.
            start = int(params.get("cursor") or 0)
            end = start + min(int(params.get("limit", 25)), self.thread_list_page_max)
            return {"data": rows[start:end], "nextCursor": str(end) if end < len(rows) else None}
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
            if th.get("archived"):
                raise ValueError(f"session {tid} is archived. Run `codex unarchive {tid}` to unarchive it first.")
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
            # Real daemon: archiving unloads the thread and moves its rollout to
            # archived_sessions/; archiving again fails because the live rollout is gone.
            tid = params["threadId"]
            th = self.threads[tid]
            if th.get("archived"):
                raise ValueError(f"no rollout found for thread id {tid}")
            th["archived"] = True
            th["_live_path"] = th.get("path")
            th["path"] = f"/mock/.codex/archived_sessions/rollout-{tid}.jsonl"
            th["status"] = {"type": "notLoaded"}
            self.loaded.discard(tid)
            return {}
        if method == "thread/unarchive":
            th = self.threads[params["threadId"]]
            th["archived"] = False
            th["path"] = th.pop("_live_path", th.get("path"))
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

    def _run_cli(self, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "codex_app_ctl.py"), "--socket", str(self.sock_path), *args],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
            env=env,
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

    def test_health_reports_newest_model_in_family(self) -> None:
        self.assertEqual(ctl.newest_in_family(["gpt-6-sol", "gpt-6.2-sol", "gpt-5.6-sol", "gpt-6.3-astra"], "gpt-6.1-sol"), "gpt-6.2-sol")
        self.assertEqual(ctl.newest_in_family(["gpt-6.10-sol", "gpt-6.9-sol"], "gpt-6.9-sol"), "gpt-6.10-sol")
        self.assertIsNone(ctl.newest_in_family(["gpt-6.2-sol"], "codex-auto-review"))

        # Unparseable configured model: reported, no verdict, no warning.
        data = json.loads(self._run_cli("health").stdout)
        self.assertIsNone(data["models"]["configuredIsNewest"])
        self.assertEqual(data["models"]["warnings"], [])

        # Configured default is the newest sol.
        self.server.config["model"] = "gpt-6.2-sol"
        self.server.models = [
            {"id": "gpt-6.2-sol", "supportedReasoningEfforts": [{"reasoningEffort": "max"}, {"reasoningEffort": "ultra"}]},
            {"id": "gpt-6.2-astra"},
            {"id": "gpt-6.1-sol"},
        ]
        fresh = json.loads(self._run_cli("health").stdout)["models"]
        self.assertTrue(fresh["configuredIsNewest"])
        self.assertEqual(fresh["supportedEfforts"], ["max", "ultra"])
        self.assertEqual(fresh["warnings"], [])

        # A newer sol ships: health flags it (found on the second page).
        self.server.models.append({"id": "gpt-6.3-sol"})
        stale = json.loads(self._run_cli("health").stdout)["models"]
        self.assertFalse(stale["configuredIsNewest"])
        self.assertEqual(stale["newestInFamily"], "gpt-6.3-sol")
        self.assertIn("gpt-6.3-sol", stale["warnings"][0])

        # Older servers without model/list still report health.
        self.server.models = None
        proc = self._run_cli("health")
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertIn("model/list not supported", json.loads(proc.stdout)["models"]["error"])

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

    def test_list_follows_next_cursor_past_the_page_cap(self) -> None:
        for i in range(250):
            tid = f"bulk-{i:04d}"
            self.server.threads[tid] = {"id": tid, "name": f"Bulk {i:04d}", "cwd": str(self.tmp_path),
                                        "status": {"type": "notLoaded"}, "path": None}
        self.server.loaded.update({"bulk-0180", "bulk-0240"})

        def listed(*args: str) -> tuple[list[str], list[dict]]:
            before = len(self.server.calls)
            proc = self._run_cli("list", *args)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            data = json.loads(proc.stdout)
            self.assertEqual(data["count"], len(data["threads"]))
            calls = [p for m, p in self.server.calls[before:] if m == "thread/list"]
            return [t["id"] for t in data["threads"]], calls

        ids, calls = listed("--limit", "500")
        self.assertEqual(ids, [f"bulk-{i:04d}" for i in range(250)])
        self.assertEqual([c.get("cursor") for c in calls], [None, "100", "200"])
        self.assertTrue(all(c["useStateDbOnly"] for c in calls))

        ids, calls = listed("--limit", "120")
        self.assertEqual(len(ids), 120)
        self.assertEqual([(c.get("cursor"), c["limit"]) for c in calls], [(None, 120), ("100", 20)])

        self.assertEqual(listed("--search", "Bulk 02", "--limit", "500")[0], [f"bulk-{i:04d}" for i in range(200, 250)])
        self.assertEqual(listed("--loaded-only", "--limit", "5")[0], ["bulk-0180", "bulk-0240"])

        # The useStateDbOnly fallback for older servers keeps paging without the flag.
        self.server.reject_state_db_only = True
        ids, calls = listed("--limit", "500")
        self.assertEqual(len(ids), 250)
        self.assertEqual([("useStateDbOnly" in c, c.get("cursor")) for c in calls],
                         [(True, None), (False, None), (False, "100"), (False, "200")])

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

    def _finish_turn(self, tid: str, text: str, status: str = "completed") -> None:
        turn = self.server.turns[tid][-1]
        turn["status"] = status
        turn["items"].append({"type": "agentMessage", "text": text})
        self.server.threads[tid]["status"] = {"type": "idle"}

    @staticmethod
    def _rollout_turn(turn_id: str, text: str | None, end: str | None = "complete") -> list[dict]:
        events: list[dict] = [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": turn_id}},
            {"type": "turn_context", "payload": {"turn_id": turn_id, "model": "gpt-test-model", "effort": "max",
                                                 "approval_policy": "on-request", "approvals_reviewer": "auto_review",
                                                 "sandbox_policy": {"type": "workspace-write"}, "cwd": "/w"}},
            {"type": "response_item", "payload": {"type": "message", "role": "user",
                                                  "content": [{"type": "input_text", "text": "Go"}]}},
        ]
        if text is not None:
            events.append({"type": "response_item", "payload": {
                "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}})
        if end == "complete":
            events.append({"type": "event_msg", "payload": {"type": "task_complete", "turn_id": turn_id}})
        return events

    def _write_rollout(self, name: str, events: list[dict]) -> Path:
        path = self.tmp_path / name
        path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
        return path

    def _park_thread(self, tid: str, rollout: Path, *, summary_text: str | None = None) -> None:
        """Make a thread look like a real notLoaded one: completed turn, summary items, rollout on disk.

        The daemon's `summary` view of a notLoaded thread's turn often carries only the
        user message, so the agent's closing text is only in the rollout.
        """
        turn = self.server.turns[tid][-1]
        turn["status"] = "completed"
        turn["itemsView"] = "summary"
        if summary_text is not None:
            turn["items"].append({"type": "agentMessage", "text": summary_text})
        self.server.threads[tid].update(status={"type": "notLoaded"}, path=str(rollout))
        self.server.loaded.discard(tid)

    def test_attention_classification(self) -> None:
        idle = {"type": "idle"}
        block = (
            "Finished the sweep and wrote results.\n\n"
            "[DECISION-NEEDED: tier=scientific]\n"
            "Question: Use the external cohort for validation?\n"
            "Options: A) external cohort | B) held-out internal split\n"
            "Recommended: B - no new data access needed"
        )
        att = ctl.classify_attention(idle, "completed", block)
        self.assertEqual(att["kind"], "decision_needed")
        self.assertEqual(att["tier"], "scientific")
        self.assertEqual(att["question"], "Use the external cohort for validation?")
        self.assertTrue(att["recommended"].startswith("B"))

        for text in (
            "All tests pass. Should I proceed with the migration?",
            "补丁已就绪。要我继续提交吗",
            "建议下一步由 Zehao 复核读取事件，再决定是否重新授权。",
            "Two candidate fixes are ready.\nWhich one do you prefer?",
        ):
            with self.subTest(text=text):
                got = ctl.classify_attention(idle, "completed", text)
                self.assertEqual((got or {}).get("kind"), "question", text)
                self.assertTrue(got["heuristic"])

        # Plain reports, running turns, and questions quoted mid-report are not flagged.
        self.assertIsNone(ctl.classify_attention(idle, "completed", "Fixed the bug; 16 tests pass."))
        quoted = "Q: should I proceed? was the old prompt.\nNow fixed.\nAll 16 tests pass.\nCommitted abc123."
        self.assertIsNone(ctl.classify_attention(idle, "completed", quoted))
        self.assertIsNone(ctl.classify_attention({"type": "active", "activeFlags": []}, "inProgress", block))

        flags = {"type": "active", "activeFlags": ["waitingOnUserInput"]}
        self.assertEqual(ctl.classify_attention(flags, "inProgress", None)["kind"], "waiting_on_user_input")
        appr = {"type": "active", "activeFlags": ["waitingOnApproval"]}
        self.assertEqual(ctl.classify_attention(appr, "inProgress", None)["kind"], "waiting_on_approval")

    def test_status_attention_and_filter(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            asking = cli.start_thread(name="Asking", cwd=str(self.tmp_path), prompt="Go")["id"]
            quiet = cli.start_thread(name="Quiet", cwd=str(self.tmp_path), prompt="Go")["id"]
            blocked = cli.start_thread(name="Blocked", cwd=str(self.tmp_path), prompt="Go")["id"]
        self._finish_turn(asking, "Done.\n[DECISION-NEEDED]\nQuestion: Delete the stale cache?\nOptions: A) yes | B) no\nRecommended: B - irreversible")
        self._finish_turn(quiet, "Done. Report written to out/report.md.")

        rollout = self.tmp_path / "rollout-blocked.jsonl"
        call = {"type": "response_item", "payload": {"type": "function_call", "name": "request_user_input",
                "arguments": json.dumps({"questions": [{"id": "q1", "question": "Which dataset?"}]})}}
        rollout.write_text(json.dumps(call) + "\n", encoding="utf-8")
        self.server.threads[blocked]["path"] = str(rollout)
        self.server.threads[blocked]["status"] = {"type": "active", "activeFlags": ["waitingOnUserInput"]}

        proc = self._run_cli("status", asking, quiet, blocked, "--attention-only")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rows = {r["id"]: r for r in map(json.loads, proc.stdout.strip().splitlines())}
        self.assertEqual(set(rows), {asking, blocked})
        self.assertEqual(rows[asking]["attention"]["question"], "Delete the stale cache?")
        self.assertEqual(rows[blocked]["attention"]["questions"][0]["question"], "Which dataset?")
        quiet_row = json.loads(self._run_cli("status", quiet).stdout)
        self.assertIsNone(quiet_row["attention"])

    def test_dispatch_interrupts_turn_waiting_on_user_input(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            created = cli.start_thread(name="Waiting", cwd=str(self.tmp_path), prompt="Go")
            tid, first = created["id"], created["turn_id"]
            self.server.threads[tid]["status"] = {"type": "active", "activeFlags": ["waitingOnUserInput"]}

            skipped = cli.dispatch(tid, "Use dataset A.", steer_only=True)
            self.assertEqual((skipped["mode"], skipped["reason"]), ("skipped", "waiting_on_user_input"))

            res = cli.dispatch(tid, "Use dataset A.")
            self.assertEqual(res["mode"], "interrupt+turn/start")
            self.assertEqual(res["interruptedTurnId"], first)
            self.assertEqual(self.server.turns[tid][0]["status"], "interrupted")
            new_turn = self.server.turns[tid][-1]
            self.assertEqual(new_turn["id"], res["turnId"])
            text = new_turn["items"][0]["content"][0]["text"]
            self.assertTrue(text.startswith("Your previous turn was interrupted"))
            self.assertTrue(text.endswith("Use dataset A."))
            self.assertNotIn("turn/steer", [m for m, _ in self.server.calls])

    def test_watch_returns_on_attention_turn_end_and_timeout(self) -> None:
        def make_client():
            return ctl.CodexAppServerClient(str(self.sock_path))

        with make_client() as cli:
            a = cli.start_thread(name="A", cwd=str(self.tmp_path), prompt="Go")["id"]
            b = cli.start_thread(name="B", cwd=str(self.tmp_path), prompt="Go")["id"]

        clock = {"t": 0.0}
        steps: list = []

        def fake_sleep(sec: float) -> None:
            clock["t"] += sec
            if steps:
                steps.pop(0)()

        def run(**kw):
            return ctl.watch_threads(make_client, [a, b], interval=10, sleep=fake_sleep, clock=lambda: clock["t"], **kw)

        # 1. A running turn that ends: turn_finished on the poll after it ends.
        steps[:] = [lambda: None, lambda: self._finish_turn(b, "Done; results in out/.")]
        r1 = run()
        self.assertFalse(r1["timedOut"])
        self.assertEqual([(e["type"], e["id"]) for e in r1["events"]], [("turn_finished", b)])
        self.assertEqual(r1["polls"], 3)

        # 2. Existing attention returns at once; --ignore-current waits for something new.
        self._finish_turn(a, "Ready.\nShould I proceed with the deploy?")
        r2 = run()
        self.assertEqual((r2["polls"], r2["events"][0]["type"], r2["events"][0]["id"]), (1, "attention", a))
        with make_client() as cli:
            cli.dispatch(b, "Next round")
        steps[:] = [lambda: self._finish_turn(b, "Next round done.\n[DECISION-NEEDED]\nQuestion: Ship it?")]
        r3 = run(ignore_current=True)
        self.assertEqual([(e["type"], e["id"]) for e in r3["events"]], [("attention", b)])
        self.assertEqual(r3["events"][0]["attention"]["kind"], "decision_needed")

        # 3. Nothing new before max_wait: timed out (CLI exit 4).
        steps[:] = []
        r4 = run(ignore_current=True, max_wait=25)
        self.assertTrue(r4["timedOut"])
        self.assertEqual(r4["polls"], 4)
        proc = self._run_cli("watch", a, "--ignore-current", "--max-wait", "0")
        self.assertEqual(proc.returncode, 4, proc.stdout + proc.stderr)
        self.assertTrue(json.loads(proc.stdout)["timedOut"])

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
        self.assertIn("[DECISION-NEEDED]", start_calls[0]["developerInstructions"])
        self.assertIn("Focus on numerical stability.", start_calls[1]["developerInstructions"])
        self.assertIn("Caption:", start_calls[1]["developerInstructions"])
        # Caption opt-out keeps the autonomy guard.
        self.assertNotIn("Caption:", start_calls[2]["developerInstructions"])
        self.assertIn("[DECISION-NEEDED]", start_calls[2]["developerInstructions"])

        # CLI `--no-caption-guide` drops only the caption guide; both flags drop everything.
        proc = self._run_cli("create", "--name", "CLI Opt-Out", "--cwd", str(self.tmp_path), "--no-caption-guide")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        last_start = [p for m, p in self.server.calls if m == "thread/start"][-1]
        self.assertNotIn("Caption:", last_start["developerInstructions"])
        proc = self._run_cli(
            "create", "--name", "CLI Bare", "--cwd", str(self.tmp_path), "--no-caption-guide", "--no-autonomy-guard"
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("developerInstructions", [p for m, p in self.server.calls if m == "thread/start"][-1])

        # A caller's own [DECISION-NEEDED...] contract (tri-autoresearch's tiered one) is not duplicated.
        tri = "Contract...\n[DECISION-NEEDED: tier=engineering|scientific|redline]\nQuestion: ..."
        composed = ctl.compose_developer_instructions(tri)
        self.assertEqual(composed.count("[DECISION-NEEDED"), 1)
        self.assertIn("Caption:", composed)

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

    def test_retire_archives_only_threads_with_nothing_left(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            ids = {
                name: cli.start_thread(name=name, cwd=str(self.tmp_path), prompt="Go")["id"]
                for name in ("Done", "Asking", "Running", "Failed", "Interrupted", "Earlier")
            }
        self._finish_turn(ids["Done"], "Report written to out/report.md. Caption: done.")
        self._finish_turn(ids["Asking"], "Done.\n[DECISION-NEEDED]\nQuestion: Delete the cache?\nOptions: A) yes | B) no\nRecommended: B - irreversible")
        self._finish_turn(ids["Failed"], "Crashed.", status="failed")
        self._finish_turn(ids["Interrupted"], "Stopped.", status="interrupted")
        self._finish_turn(ids["Earlier"], "Done.")
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            cli.archive_thread(ids["Earlier"])

        registry = self.tmp_path / "FLEET-REGISTRY.json"
        registry.write_text(json.dumps({"sessions": [{"slot": i, "id": tid} for i, tid in enumerate(ids.values())]}), encoding="utf-8")

        def run(*extra: str) -> dict[str, dict]:
            proc = self._run_cli("retire", "--registry", str(registry), *extra)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            return {r["name"]: r for r in map(json.loads, proc.stdout.strip().splitlines())}

        preview = run("--dry-run")
        self.assertEqual(preview["Done"]["action"], "would_archive")
        self.assertFalse(self.server.threads[ids["Done"]].get("archived"))
        expected_skips = {
            "Asking": "needs_attention:decision_needed",
            "Running": "turn_in_progress",
            "Failed": "last_turn_failed",
            "Interrupted": "last_turn_interrupted",
            "Earlier": "already_archived",
        }
        for name, reason in expected_skips.items():
            with self.subTest(name=name):
                self.assertEqual((preview[name]["action"], preview[name]["reason"]), ("skipped", reason))

        applied = run()
        self.assertEqual(applied["Done"]["action"], "archived")
        self.assertTrue(self.server.threads[ids["Done"]]["archived"])
        self.assertFalse(self.server.threads[ids["Asking"]].get("archived"))
        self.assertEqual(json.loads(self._run_cli("status", ids["Done"]).stdout)["archived"], True)

        # --force covers questions and failed turns, never a running turn.
        forced = run("--force")
        for name in ("Asking", "Failed", "Interrupted"):
            self.assertEqual(forced[name]["action"], "archived", name)
        self.assertEqual(forced["Running"]["reason"], "turn_in_progress")
        self.assertEqual(forced["Done"]["reason"], "already_archived")

        missing = self._run_cli("retire")
        self.assertNotEqual(missing.returncode, 0)

    def test_attention_falls_back_to_rollout_when_turn_summary_has_no_agent_message(self) -> None:
        decision = ("Sweep finished.\n[DECISION-NEEDED]\nQuestion: Drop the stale cohort?\n"
                    "Options: A) drop | B) keep\nRecommended: B - irreversible")
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            ids = {name: cli.start_thread(name=name, cwd=str(self.tmp_path), prompt="Go")["id"]
                   for name in ("Deciding", "Asking", "Done", "Summarised")}
        self._park_thread(ids["Deciding"], self._write_rollout(
            "deciding.jsonl", self._rollout_turn("t1", "Earlier round done.") + self._rollout_turn("t2", decision)))
        self._park_thread(ids["Asking"], self._write_rollout(
            "asking.jsonl", self._rollout_turn("t1", "Patch is ready. Should I proceed with the migration?")))
        self._park_thread(ids["Done"], self._write_rollout(
            "done.jsonl", self._rollout_turn("t1", "Report written to out/report.md.")))
        # A summary that does carry the agent message wins over the rollout.
        self._park_thread(ids["Summarised"], self._write_rollout(
            "summarised.jsonl", self._rollout_turn("t1", decision)), summary_text="All done; nothing pending.")

        proc = self._run_cli("status", *ids.values(), "--last-message")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rows = {r["name"]: r for r in map(json.loads, proc.stdout.strip().splitlines())}
        deciding = rows["Deciding"]["attention"]
        self.assertEqual((deciding["kind"], deciding["message_source"]), ("decision_needed", "rollout"))
        self.assertEqual(deciding["question"], "Drop the stale cohort?")
        self.assertTrue(rows["Deciding"]["last_agent_message"].startswith("Sweep finished."))
        self.assertEqual((rows["Asking"]["attention"]["kind"], rows["Asking"]["attention"]["message_source"]),
                         ("question", "rollout"))
        self.assertIsNone(rows["Done"]["attention"])
        self.assertIsNone(rows["Summarised"]["attention"])
        self.assertEqual(rows["Summarised"]["last_agent_message"], "All done; nothing pending.")

        # retire keeps the threads that wait on a reply and archives the finished one.
        proc = self._run_cli("retire", *ids.values())
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        retired = {r["name"]: r for r in map(json.loads, proc.stdout.strip().splitlines())}
        self.assertEqual(retired["Deciding"]["reason"], "needs_attention:decision_needed")
        self.assertEqual(retired["Asking"]["reason"], "needs_attention:question")
        self.assertEqual(retired["Done"]["action"], "archived")
        self.assertEqual(retired["Summarised"]["action"], "archived")
        self.assertFalse(self.server.threads[ids["Deciding"]].get("archived"))
        self.assertFalse(self.server.threads[ids["Asking"]].get("archived"))

    @unittest.skipUnless(shutil.which("zstd"), "needs the zstd CLI")
    def test_compressed_rollouts_are_read_through_zstd(self) -> None:
        # Codex compresses older rollouts to `<path>.zst`, but thread/read keeps reporting `<path>`.
        decision = "Done.\n[DECISION-NEEDED]\nQuestion: Publish the report?\nOptions: A) yes | B) no\nRecommended: B - external"
        plain = self._write_rollout("rollout-old.jsonl", self._rollout_turn("t1", decision))
        subprocess.run(["zstd", "-q", "--rm", str(plain)], check=True)
        self.assertFalse(plain.exists())
        self.assertTrue(Path(f"{plain}.zst").exists())

        call = {"type": "response_item", "payload": {"type": "function_call", "name": "request_user_input",
                "arguments": json.dumps({"questions": [{"id": "q1", "question": "Which dataset?"}]})}}
        waiting_rollout = self._write_rollout("rollout-waiting.jsonl", [{"type": "event_msg", "payload": {"type": "warning", "message": "x" * 300}}] * 400 + [call])
        subprocess.run(["zstd", "-q", "--rm", str(waiting_rollout)], check=True)

        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            tid = cli.start_thread(name="Compressed", cwd=str(self.tmp_path), prompt="Go")["id"]
            waiting = cli.start_thread(name="Waiting", cwd=str(self.tmp_path), prompt="Go")["id"]
        self._park_thread(tid, plain)
        self.server.threads[waiting].update(path=str(waiting_rollout),
                                            status={"type": "active", "activeFlags": ["waitingOnUserInput"]})

        offline = json.loads(self._run_cli("inspect-rollout", "--path", str(plain)).stdout)
        self.assertTrue(offline["exists"])
        self.assertTrue(offline["compressed"])
        self.assertEqual(offline["path"], str(plain))
        self.assertEqual(offline["last_turn_status"], "completed")
        self.assertEqual(offline["last_turn_context"]["approvals_reviewer"], "auto_review")
        self.assertEqual(json.loads(self._run_cli("inspect-rollout", "--thread", tid).stdout), offline)

        st = json.loads(self._run_cli("status", tid, "--verify-policy").stdout)
        self.assertEqual(st["rollout_turn_context"]["approval_policy"], "on-request")
        self.assertEqual((st["attention"]["kind"], st["attention"]["question"]), ("decision_needed", "Publish the report?"))
        waiting_st = json.loads(self._run_cli("status", waiting).stdout)
        self.assertEqual(waiting_st["attention"]["questions"][0]["question"], "Which dataset?")

        cap = json.loads(self._run_cli("caption", tid, "--json").stdout)
        self.assertEqual(cap["caption_source"], "latest_message")
        self.assertIn("Publish the report?", cap["caption"])
        offline_cap = self._run_cli("caption", "--path", f"{plain}.zst")
        self.assertEqual(offline_cap.returncode, 0, offline_cap.stdout + offline_cap.stderr)
        self.assertTrue(offline_cap.stdout.startswith("rollout-old — offline (last turn completed)"), offline_cap.stdout)

        retired = json.loads(self._run_cli("retire", tid).stdout)
        self.assertEqual(retired["reason"], "needs_attention:decision_needed")

        # A damaged compressed rollout is an error, not an empty or missing rollout.
        Path(self.tmp_path / "rollout-bad.jsonl.zst").write_bytes(b"not zstd data")
        bad = self._run_cli("inspect-rollout", "--path", str(self.tmp_path / "rollout-bad.jsonl"))
        self.assertEqual(bad.returncode, 1, bad.stdout + bad.stderr)
        err = json.loads(bad.stdout)
        self.assertEqual(err["error"], "RolloutReadError")
        self.assertIn("zstd could not decompress", err["detail"])

    def test_compressed_rollout_without_zstd_fails_clearly(self) -> None:
        rollout = self.tmp_path / "rollout-old.jsonl"
        Path(f"{rollout}.zst").write_bytes(b"(zstd frame)")  # never decompressed: zstd is missing
        no_zstd = dict(os.environ, PATH=str(self.tmp_path / "empty-bin"))

        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            tid = cli.start_thread(name="Compressed", cwd=str(self.tmp_path), prompt="Go")["id"]
        self._park_thread(tid, rollout)

        for args in (("inspect-rollout", "--path", str(rollout)), ("inspect-rollout", "--thread", tid),
                     ("status", tid), ("caption", tid), ("caption", "--path", str(rollout))):
            with self.subTest(args=args[:2]):
                proc = self._run_cli(*args, env=no_zstd)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertNotIn("Traceback", proc.stderr)
                err = json.loads(proc.stdout)
                self.assertEqual(err["error"], "RolloutReadError")
                self.assertIn("`zstd` command is not on PATH", err["detail"])

        # retire cannot check the thread's attention, so it must not archive it.
        self._run_cli("retire", tid, env=no_zstd)
        self.assertFalse(self.server.threads[tid].get("archived"))

    def test_dispatch_unarchives_archived_thread_first(self) -> None:
        with ctl.CodexAppServerClient(str(self.sock_path)) as cli:
            tid = cli.start_thread(name="Retired", cwd=str(self.tmp_path), prompt="Go")["id"]
            self._finish_turn(tid, "Done.")
            self.assertEqual(cli.retire_thread(tid)["action"], "archived")
            self.assertEqual(cli.thread_status(tid)["status_type"], "notLoaded")

            res = cli.dispatch(tid, "One more round: add tests.")
            self.assertEqual(res["mode"], "turn/start")
            self.assertTrue(res["resumedFromNotLoaded"])
            self.assertTrue(res["unarchived"])
            self.assertFalse(self.server.threads[tid]["archived"])
            self.assertFalse(cli.thread_status(tid)["archived"])
            methods = [m for m, _ in self.server.calls]
            last_turn_start = len(methods) - 1 - methods[::-1].index("turn/start")
            self.assertLess(methods.index("thread/unarchive"), last_turn_start)

            # Steer-only never revives an archived thread.
            self._finish_turn(tid, "Done again.")
            cli.archive_thread(tid)
            skipped = cli.dispatch(tid, "FYI", steer_only=True)
            self.assertEqual(skipped["reason"], "thread_not_loaded")
            self.assertTrue(self.server.threads[tid]["archived"])

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
