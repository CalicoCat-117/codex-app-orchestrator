#!/usr/bin/env python3
"""Zero-dependency official Codex app-server WebSocket JSON-RPC controller.

Orchestrates persistent, UI-visible Codex sessions via the local app-server
daemon's Unix-domain WebSocket control socket. Supports fleet creation,
non-blocking dispatch (`turn/steer` for active turns, `thread/resume` +
`turn/start` for unloaded threads, `turn/start` for idle threads), Guardian
`auto_review` policy management, rollout policy verification, quota health
checks, and lifecycle teardown.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

CLIENT_INFO = {
    "name": "codex-app-orchestrator",
    "title": "Codex App Orchestrator",
    "version": "1.0.0",
}

VALID_APPROVAL_POLICIES = ("on-request", "untrusted", "never")
VALID_REVIEWERS = ("auto_review", "user")
VALID_SANDBOXES = ("workspace-write", "read-only", "danger-full-access")


class CodexRPCError(RuntimeError):
    """Structured error returned by a Codex app-server JSON-RPC call."""

    def __init__(self, method: str, error: Any) -> None:
        self.method = method
        self.error = error
        if isinstance(error, dict):
            self.code = error.get("code")
            self.rpc_message = error.get("message", str(error))
            self.data = error.get("data")
        else:
            self.code = None
            self.rpc_message = str(error)
            self.data = None
        super().__init__(f"RPC {method} failed: {self.rpc_message}")


class QuotaExhaustedError(RuntimeError):
    """Raised when `--check-quota` detects that ordinary Codex usage is blocked."""

    def __init__(self, rate_limits: dict[str, Any]) -> None:
        self.rate_limits = rate_limits
        super().__init__(
            "Codex quota exhausted or rate-limited; ordinaryUsageAllowed is false."
        )


def build_sandbox_policy(
    sandbox: str = "workspace-write",
    *,
    writable_roots: list[str] | None = None,
    network_access: bool = False,
    exclude_slash_tmp: bool = False,
    exclude_tmpdir_env_var: bool = False,
) -> dict[str, Any]:
    """Convert CLI sandbox mode name into the app-server `SandboxPolicy` object."""
    if sandbox == "workspace-write":
        return {
            "type": "workspaceWrite",
            "writableRoots": list(writable_roots or []),
            "networkAccess": bool(network_access),
            "excludeSlashTmp": bool(exclude_slash_tmp),
            "excludeTmpdirEnvVar": bool(exclude_tmpdir_env_var),
        }
    if sandbox == "read-only":
        return {
            "type": "readOnly",
            "networkAccess": bool(network_access),
        }
    if sandbox == "danger-full-access":
        return {"type": "dangerFullAccess"}
    raise ValueError(
        f"Unsupported sandbox mode {sandbox!r}; expected one of {VALID_SANDBOXES}"
    )


def get_daemon_info(codex_bin: str = "codex") -> dict[str, Any] | None:
    """Query `codex app-server daemon version` for live daemon status and socketPath."""
    try:
        proc = subprocess.run(
            [codex_bin, "app-server", "daemon", "version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def resolve_socket_path(
    explicit_socket: str | None = None,
    *,
    codex_bin: str = "codex",
) -> str:
    """Discover the active Codex app-server Unix control socket."""
    if explicit_socket:
        return explicit_socket

    env_sock = os.environ.get("CODEX_APP_SERVER_SOCKET")
    if env_sock:
        return env_sock

    info = get_daemon_info(codex_bin=codex_bin)
    if info and info.get("socketPath"):
        status = info.get("status")
        sock = str(info["socketPath"])
        if status == "running" and os.path.exists(sock):
            return sock
        if status and status != "running":
            raise RuntimeError(
                f"Codex app-server daemon status is {status!r} (expected 'running'). "
                "Start it with: codex app-server daemon start"
            )
        if os.path.exists(sock):
            return sock

    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    fallback = codex_home / "app-server-control" / "app-server-control.sock"
    if fallback.exists():
        return str(fallback)

    raise FileNotFoundError(
        f"Codex app-server control socket not found (checked daemon version and {fallback}). "
        "Start the daemon with: codex app-server daemon start"
    )


def inspect_rollout_file(path: str | Path, last_n: int = 5) -> dict[str, Any]:
    """Read-only local inspection of a Codex rollout JSONL file.

    Extracts the latest `turn_context` (ground-truth effective approval policy,
    approvals reviewer, sandbox policy, model, effort), recent assistant messages,
    and any error/rate-limit events.
    """
    p = Path(path)
    if not p.exists():
        return {"exists": False, "path": str(p)}

    last_turn_context: dict[str, Any] | None = None
    turn_context_count = 0
    assistant_messages: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    task_completes = 0

    with p.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            entry_type = obj.get("type")
            payload = obj.get("payload")
            if entry_type == "turn_context" and isinstance(payload, dict):
                turn_context_count += 1
                last_turn_context = {
                    "turn_id": payload.get("turn_id"),
                    "model": payload.get("model"),
                    "effort": payload.get("effort"),
                    "approval_policy": payload.get("approval_policy"),
                    "approvals_reviewer": payload.get("approvals_reviewer"),
                    "sandbox_policy": payload.get("sandbox_policy"),
                    "cwd": payload.get("cwd"),
                }
            elif entry_type == "response_item" and isinstance(payload, dict):
                if payload.get("type") == "message" and payload.get("role") == "assistant":
                    texts = []
                    for part in payload.get("content") or []:
                        if isinstance(part, dict) and isinstance(part.get("text"), str):
                            texts.append(part["text"])
                    if texts:
                        assistant_messages.append(
                            {
                                "timestamp": obj.get("timestamp"),
                                "text": "\n".join(texts),
                            }
                        )
            elif entry_type == "event_msg" and isinstance(payload, dict):
                ev_type = payload.get("type")
                if ev_type == "task_complete":
                    task_completes += 1
                elif ev_type in ("error", "turn_aborted", "warning"):
                    errors.append(
                        {
                            "timestamp": obj.get("timestamp"),
                            "type": ev_type,
                            "message": payload.get("message") or payload.get("reason") or payload,
                        }
                    )

    trimmed_messages = [
        {
            "timestamp": m["timestamp"],
            "preview": m["text"][:500],
            "length": len(m["text"]),
        }
        for m in assistant_messages[-max(1, last_n) :]
    ]

    return {
        "exists": True,
        "path": str(p),
        "size_bytes": p.stat().st_size,
        "turn_context_count": turn_context_count,
        "task_complete_count": task_completes,
        "last_turn_context": last_turn_context,
        "recent_assistant_messages": trimmed_messages,
        "recent_errors": errors[-max(1, last_n) :],
    }


def _extract_agent_message_from_turn(turn: dict[str, Any] | None) -> str | None:
    """Extract the last assistant text preview from a `thread/turns/list` turn item."""
    if not isinstance(turn, dict):
        return None
    items = turn.get("items") or []
    for item in reversed(items):
        if not isinstance(item, dict):
            continue
        if item.get("type") in ("agentMessage", "assistantMessage", "message"):
            if isinstance(item.get("text"), str) and item["text"].strip():
                return item["text"]
            content = item.get("content")
            if isinstance(content, list):
                parts = [
                    p.get("text", "")
                    for p in content
                    if isinstance(p, dict) and isinstance(p.get("text"), str)
                ]
                joined = "\n".join(x for x in parts if x)
                if joined.strip():
                    return joined
    return None


def _is_turn_steer_race_error(exc: CodexRPCError) -> bool:
    """Return True if a `turn/steer` error indicates the target turn already ended."""
    msg = (exc.rpc_message or "").lower()
    indicators = (
        "expectedturnid",
        "expected turn",
        "no active turn",
        "not active",
        "already completed",
        "turn mismatch",
        "no in-progress turn",
        "no in progress turn",
    )
    return any(tok in msg for tok in indicators)


class CodexAppServerClient:
    """RFC 6455 WebSocket JSON-RPC client over the Codex app-server Unix socket."""

    def __init__(
        self,
        sock_path: str | None = None,
        *,
        timeout: float = 20.0,
        client_info: dict[str, str] | None = None,
        codex_bin: str = "codex",
    ) -> None:
        self.codex_bin = codex_bin
        self.sock_path = resolve_socket_path(sock_path, codex_bin=codex_bin)
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect(self.sock_path)
        self.rbuf = bytearray()
        self.next_id = 1
        self._closed = False
        self.server_info: dict[str, Any] = {}
        try:
            self._ws_handshake()
            self.server_info = self.rpc(
                "initialize",
                {
                    "clientInfo": client_info or CLIENT_INFO,
                    "capabilities": {"experimentalApi": False},
                },
            )
            self._ws_send_json({"method": "initialized", "params": {}})
        except Exception:
            self.close()
            raise

    def __enter__(self) -> CodexAppServerClient:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._ws_send_frame(0x8, b"")
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass

    def _ws_handshake(self) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        req = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode("ascii"))
        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise EOFError("WebSocket handshake closed before HTTP headers completed")
            buf.extend(chunk)
        sep = buf.index(b"\r\n\r\n")
        header = bytes(buf[:sep])
        self.rbuf.extend(buf[sep + 4 :])
        status_line = header.splitlines()[0] if header else b""
        if b" 101 " not in status_line and not status_line.endswith(b" 101"):
            raise RuntimeError(f"Unexpected WebSocket handshake response: {status_line!r}")

    def _ws_send_frame(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        out = bytearray([0x80 | (opcode & 0x0F)])
        length = len(payload)
        if length < 126:
            out.append(0x80 | length)
        elif length < 65536:
            out.append(0x80 | 126)
            out.extend(struct.pack("!H", length))
        else:
            out.append(0x80 | 127)
            out.extend(struct.pack("!Q", length))
        out.extend(mask)
        if payload:
            out.extend(bytes(b ^ mask[i & 3] for i, b in enumerate(payload)))
        self.sock.sendall(out)

    def _ws_send_json(self, obj: dict[str, Any]) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._ws_send_frame(0x1, data)

    def _read_exact(self, n: int) -> bytes:
        while len(self.rbuf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("Codex app-server socket closed unexpectedly")
            self.rbuf.extend(chunk)
        out = bytes(self.rbuf[:n])
        del self.rbuf[:n]
        return out

    def _ws_recv_json(self) -> dict[str, Any]:
        fragments: list[bytes] = []
        expecting_continuation = False
        while True:
            b1, b2 = self._read_exact(2)
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(length) if length else b""
            if mask and payload:
                payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))

            # Control frames may be injected mid-fragment per RFC 6455.
            if opcode == 0x9:  # PING
                self._ws_send_frame(0xA, payload[:125])
                continue
            if opcode == 0xA:  # PONG
                continue
            if opcode == 0x8:  # CLOSE
                raise EOFError("Codex app-server closed WebSocket connection")

            if opcode == 0x1:  # TEXT
                fragments = [payload]
                if fin:
                    return json.loads(payload.decode("utf-8"))
                expecting_continuation = True
            elif opcode == 0x0 and expecting_continuation:  # CONTINUATION
                fragments.append(payload)
                if fin:
                    full = b"".join(fragments)
                    fragments = []
                    expecting_continuation = False
                    return json.loads(full.decode("utf-8"))
            else:
                # Ignore unsupported non-text data frames defensively.
                expecting_continuation = False
                fragments = []

    def rpc(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        req_id = self.next_id
        self.next_id += 1
        self._ws_send_json({"id": req_id, "method": method, "params": params or {}})
        while True:
            msg = self._ws_recv_json()
            if msg.get("id") == req_id:
                if "error" in msg and msg["error"] is not None:
                    raise CodexRPCError(method, msg["error"])
                result = msg.get("result")
                return result if isinstance(result, dict) else {"value": result}

    # --- High-level protocol helpers ---

    def read_config(self) -> dict[str, Any]:
        res = self.rpc("config/read", {})
        cfg = res.get("config")
        return cfg if isinstance(cfg, dict) else res

    def read_rate_limits(self) -> dict[str, Any]:
        return self.rpc("account/rateLimits/read", {})

    def list_loaded_threads(self) -> list[str]:
        res = self.rpc("thread/loaded/list", {})
        data = res.get("data")
        return list(data) if isinstance(data, list) else []

    def health(self) -> dict[str, Any]:
        daemon = get_daemon_info(codex_bin=self.codex_bin)
        cfg = self.read_config()
        try:
            rate_limits = self.read_rate_limits()
        except CodexRPCError as exc:
            rate_limits = {"error": exc.rpc_message}
        loaded = self.list_loaded_threads()

        ordinary_allowed = rate_limits.get("ordinaryUsageAllowed")
        rl_obj = rate_limits.get("rateLimits") if isinstance(rate_limits, dict) else None
        primary = rl_obj.get("primary") if isinstance(rl_obj, dict) else None

        return {
            "socketPath": self.sock_path,
            "daemon": daemon,
            "serverInfo": self.server_info,
            "defaults": {
                "model": cfg.get("model"),
                "effort": cfg.get("model_reasoning_effort"),
                "approvalPolicy": cfg.get("approval_policy"),
                "approvalsReviewer": cfg.get("approvals_reviewer"),
                "sandboxMode": cfg.get("sandbox_mode"),
            },
            "quota": {
                "ordinaryUsageAllowed": ordinary_allowed,
                "usedPercent": primary.get("usedPercent") if isinstance(primary, dict) else None,
                "resetsAt": primary.get("resetsAt") if isinstance(primary, dict) else None,
                "rateLimitReachedType": rl_obj.get("rateLimitReachedType") if isinstance(rl_obj, dict) else None,
                "planType": rl_obj.get("planType") if isinstance(rl_obj, dict) else None,
            },
            "loadedThreadsCount": len(loaded),
            "loadedThreads": loaded,
        }

    def check_quota_or_raise(self) -> dict[str, Any]:
        rl = self.read_rate_limits()
        allowed = rl.get("ordinaryUsageAllowed")
        reached = (rl.get("rateLimits") or {}).get("rateLimitReachedType")
        if allowed is False or reached not in (None, "", "none"):
            raise QuotaExhaustedError(rl)
        return rl

    def list_threads(
        self,
        *,
        search: str | None = None,
        cwd: str | None = None,
        limit: int = 25,
        archived: bool | None = None,
        loaded_only: bool = False,
        use_state_db_only: bool = True,
    ) -> list[dict[str, Any]]:
        loaded_set = set(self.list_loaded_threads())
        params: dict[str, Any] = {"limit": limit}
        if search:
            params["searchTerm"] = search
        if cwd:
            params["cwd"] = cwd
        if archived is not None:
            params["archived"] = archived
        if use_state_db_only:
            params["useStateDbOnly"] = True
        try:
            res = self.rpc("thread/list", params)
        except CodexRPCError:
            if not use_state_db_only:
                raise
            # Compatibility fallback for older app-server builds without `useStateDbOnly`.
            params.pop("useStateDbOnly", None)
            res = self.rpc("thread/list", params)
        rows = res.get("data") or []
        out = []
        for th in rows:
            tid = th.get("id")
            is_loaded = tid in loaded_set
            if loaded_only and not is_loaded:
                continue
            out.append(
                {
                    "id": tid,
                    "name": th.get("name"),
                    "cwd": th.get("cwd"),
                    "model": th.get("model"),
                    "reasoningEffort": th.get("reasoningEffort"),
                    "status": th.get("status"),
                    "loaded": is_loaded,
                    "source": th.get("source"),
                    "updatedAt": th.get("updatedAt"),
                    "path": th.get("path"),
                }
            )
        return out

    def read_thread(self, tid: str, *, include_turns: bool = False) -> dict[str, Any]:
        res = self.rpc("thread/read", {"threadId": tid, "includeTurns": include_turns})
        return res.get("thread") or res

    def latest_turn(self, tid: str) -> dict[str, Any] | None:
        """Fetch only the most recent turn via `thread/turns/list` (O(1) payload)."""
        try:
            res = self.rpc(
                "thread/turns/list",
                {"threadId": tid, "limit": 1, "sortDirection": "desc"},
            )
            data = res.get("data") or []
            return data[0] if data else None
        except CodexRPCError:
            # Compatibility fallback for older servers without `thread/turns/list`.
            th = self.read_thread(tid, include_turns=True)
            turns = th.get("turns") or []
            return turns[-1] if turns else None

    def thread_status(
        self,
        tid: str,
        *,
        verify_policy: bool = False,
        include_last_message: bool = False,
    ) -> dict[str, Any]:
        th = self.read_thread(tid, include_turns=False)
        lt = self.latest_turn(tid)
        status_obj = th.get("status")
        status_type = status_obj.get("type") if isinstance(status_obj, dict) else status_obj
        out: dict[str, Any] = {
            "id": th.get("id") or tid,
            "name": th.get("name"),
            "cwd": th.get("cwd"),
            "model": th.get("model"),
            "reasoningEffort": th.get("reasoningEffort"),
            "source": th.get("source"),
            "status": status_obj,
            "status_type": status_type,
            "last_turn_id": lt.get("id") if isinstance(lt, dict) else None,
            "last_turn_status": lt.get("status") if isinstance(lt, dict) else None,
            "last_turn_error": lt.get("error") if isinstance(lt, dict) else None,
            "updatedAt": th.get("updatedAt"),
            "path": th.get("path"),
        }
        if include_last_message:
            msg = _extract_agent_message_from_turn(lt)
            if msg is not None:
                out["last_agent_message"] = msg[:1000]
        if verify_policy and th.get("path"):
            rollout = inspect_rollout_file(th["path"], last_n=3)
            out["rollout_turn_context"] = rollout.get("last_turn_context")
            out["rollout_recent_errors"] = rollout.get("recent_errors")
            if include_last_message and "last_agent_message" not in out:
                msgs = rollout.get("recent_assistant_messages") or []
                if msgs:
                    out["last_agent_message"] = msgs[-1]["preview"]
        return out

    def start_thread(
        self,
        *,
        name: str,
        cwd: str,
        prompt: str | None = None,
        model: str | None = None,
        effort: str | None = None,
        approval_policy: str = "on-request",
        approvals_reviewer: str = "auto_review",
        sandbox: str = "workspace-write",
        writable_roots: list[str] | None = None,
        network_access: bool = False,
        developer_instructions: str | None = None,
        ephemeral: bool = False,
    ) -> dict[str, Any]:
        """Create a persistent, Codex App-visible thread and optionally start Turn 1."""
        start_params: dict[str, Any] = {
            "cwd": str(Path(cwd).resolve()),
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
            "sandbox": sandbox,
            "ephemeral": bool(ephemeral),
        }
        cfg: dict[str, Any] = {}
        if model:
            start_params["model"] = model
            cfg["model"] = model
        if effort:
            cfg["model_reasoning_effort"] = effort
        if cfg:
            start_params["config"] = cfg
        if developer_instructions:
            start_params["developerInstructions"] = developer_instructions

        started = self.rpc("thread/start", start_params)
        tid = started["thread"]["id"]
        self.rpc("thread/name/set", {"threadId": tid, "name": name})

        turn_id = None
        if prompt is not None:
            turn_params: dict[str, Any] = {
                "threadId": tid,
                "cwd": str(Path(cwd).resolve()),
                "approvalPolicy": approval_policy,
                "approvalsReviewer": approvals_reviewer,
                "sandboxPolicy": build_sandbox_policy(
                    sandbox,
                    writable_roots=writable_roots,
                    network_access=network_access,
                ),
                "input": [{"type": "text", "text": prompt}],
            }
            if model:
                turn_params["model"] = model
            if effort:
                turn_params["effort"] = effort
            turn_res = self.rpc("turn/start", turn_params)
            turn_id = (turn_res.get("turn") or {}).get("id")

        info = self.read_thread(tid, include_turns=False)
        return {
            "id": tid,
            "name": info.get("name") or name,
            "cwd": info.get("cwd"),
            "model": info.get("model") or started.get("model"),
            "reasoningEffort": info.get("reasoningEffort") or started.get("reasoningEffort") or effort,
            "status": info.get("status"),
            "source": info.get("source"),
            "path": info.get("path"),
            "turn_id": turn_id,
            "approvalPolicy": started.get("approvalPolicy", approval_policy),
            "approvalsReviewer": started.get("approvalsReviewer", approvals_reviewer),
            "sandbox": started.get("sandbox", sandbox),
        }

    def apply_policy(
        self,
        tid: str,
        *,
        model: str | None = None,
        effort: str | None = None,
        approval_policy: str = "on-request",
        approvals_reviewer: str = "auto_review",
        sandbox: str = "workspace-write",
    ) -> dict[str, Any]:
        """Apply policy/model via `thread/resume` (loaded threads also take per-turn override on `turn/start`)."""
        loaded_before = tid in set(self.list_loaded_threads())
        params: dict[str, Any] = {
            "threadId": tid,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
            "sandbox": sandbox,
            "excludeTurns": True,
        }
        if model:
            params["model"] = model
        if effort:
            params["config"] = {"model_reasoning_effort": effort}

        res = self.rpc("thread/resume", params)
        th = res.get("thread") or {}
        effective_reviewer = res.get("approvalsReviewer")
        note = None
        if loaded_before and effective_reviewer != approvals_reviewer:
            note = (
                "Thread was already loaded in memory; app-server preserves cached thread "
                "settings until the next `turn/start` passes explicit policy overrides."
            )
        return {
            "id": tid,
            "name": th.get("name"),
            "status": th.get("status"),
            "wasLoaded": loaded_before,
            "model": res.get("model"),
            "reasoningEffort": res.get("reasoningEffort"),
            "approvalPolicy": res.get("approvalPolicy"),
            "approvalsReviewer": effective_reviewer,
            "sandbox": res.get("sandbox"),
            "note": note,
        }

    def dispatch(
        self,
        tid: str,
        message: str,
        *,
        model: str | None = None,
        effort: str | None = None,
        approval_policy: str = "on-request",
        approvals_reviewer: str = "auto_review",
        sandbox: str = "workspace-write",
        writable_roots: list[str] | None = None,
        network_access: bool = False,
        cwd: str | None = None,
        steer_only: bool = False,
        queue_only: bool = False,
        check_quota: bool = False,
    ) -> dict[str, Any]:
        """Non-blocking dispatch: `turn/steer` if active, `thread/resume` + `turn/start` if unloaded, `turn/start` if idle."""
        if check_quota:
            self.check_quota_or_raise()

        if queue_only:
            proc = subprocess.run(
                [self.codex_bin, "queue", "--thread", tid, "--resume", "--message", message],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"`codex queue` failed (rc={proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}"
                )
            return {
                "mode": "codex-queue",
                "threadId": tid,
                "stdout": proc.stdout.strip(),
                "stderr": proc.stderr.strip(),
            }

        th = self.read_thread(tid, include_turns=False)
        status_type = (th.get("status") or {}).get("type") if isinstance(th.get("status"), dict) else th.get("status")
        resumed = False
        if status_type == "notLoaded":
            if steer_only:
                return {
                    "mode": "skipped",
                    "reason": "thread_not_loaded",
                    "threadId": tid,
                    "status": th.get("status"),
                }
            self.apply_policy(
                tid,
                model=model,
                effort=effort,
                approval_policy=approval_policy,
                approvals_reviewer=approvals_reviewer,
                sandbox=sandbox,
            )
            resumed = True
            th = self.read_thread(tid, include_turns=False)

        lt = self.latest_turn(tid)
        if lt and lt.get("status") == "inProgress" and lt.get("id"):
            expected_turn_id = lt["id"]
            try:
                res = self.rpc(
                    "turn/steer",
                    {
                        "threadId": tid,
                        "expectedTurnId": expected_turn_id,
                        "input": [{"type": "text", "text": message}],
                    },
                )
                return {
                    "mode": "turn/steer",
                    "threadId": tid,
                    "expectedTurnId": expected_turn_id,
                    "turnId": res.get("turnId") or expected_turn_id,
                    "result": res,
                }
            except CodexRPCError as exc:
                if steer_only or not _is_turn_steer_race_error(exc):
                    raise
                # Race condition: active turn finished right before `turn/steer`.
                lt_retry = self.latest_turn(tid)
                if lt_retry and lt_retry.get("status") == "inProgress":
                    raise

        if steer_only:
            return {
                "mode": "skipped",
                "reason": "no_active_turn",
                "threadId": tid,
                "status": th.get("status"),
                "lastTurnStatus": lt.get("status") if lt else None,
            }

        turn_params: dict[str, Any] = {
            "threadId": tid,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
            "sandboxPolicy": build_sandbox_policy(
                sandbox,
                writable_roots=writable_roots,
                network_access=network_access,
            ),
            "cwd": str(Path(cwd or th.get("cwd") or os.getcwd()).resolve()),
            "input": [{"type": "text", "text": message}],
        }
        if model:
            turn_params["model"] = model
        if effort:
            turn_params["effort"] = effort

        res = self.rpc("turn/start", turn_params)
        return {
            "mode": "turn/start",
            "resumedFromNotLoaded": resumed,
            "threadId": tid,
            "turnId": (res.get("turn") or {}).get("id"),
        }

    def interrupt_turn(self, tid: str, turn_id: str | None = None) -> dict[str, Any]:
        target_turn_id = turn_id
        if not target_turn_id:
            lt = self.latest_turn(tid)
            if not lt or lt.get("status") != "inProgress" or not lt.get("id"):
                return {
                    "threadId": tid,
                    "interrupted": False,
                    "reason": "no_active_turn",
                    "lastTurnStatus": lt.get("status") if lt else None,
                }
            target_turn_id = lt["id"]
        res = self.rpc("turn/interrupt", {"threadId": tid, "turnId": target_turn_id})
        return {
            "threadId": tid,
            "turnId": target_turn_id,
            "interrupted": True,
            "result": res,
        }

    def archive_thread(self, tid: str) -> dict[str, Any]:
        res = self.rpc("thread/archive", {"threadId": tid})
        return {"threadId": tid, "archived": True, "result": res}

    def unarchive_thread(self, tid: str) -> dict[str, Any]:
        res = self.rpc("thread/unarchive", {"threadId": tid})
        return {"threadId": tid, "unarchived": True, "result": res}

    def delete_thread(self, tid: str) -> dict[str, Any]:
        res = self.rpc("thread/delete", {"threadId": tid})
        return {"threadId": tid, "deleted": True, "result": res}


def validate_fleet_spec(specs: list[dict[str, Any]], spec_dir: Path) -> list[dict[str, Any]]:
    """Validate a multi-session fleet launch specification before touching the daemon.

    Enforces isolation invariants:
    - Each item must declare `name` and `cwd`.
    - Distinct writer slots must not share the same `worktree` or `output_dir`.
    - `prompt_file` paths (if provided) must exist.
    """
    if not isinstance(specs, list) or not specs:
        raise ValueError("Fleet spec must be a non-empty JSON array of session specs.")

    seen_worktrees: dict[str, str] = {}
    seen_outdirs: dict[str, str] = {}
    normalized: list[dict[str, Any]] = []

    for idx, raw in enumerate(specs):
        if not isinstance(raw, dict):
            raise ValueError(f"Spec entry #{idx} must be a JSON object.")
        name = raw.get("name")
        cwd = raw.get("cwd")
        if not name or not isinstance(name, str):
            raise ValueError(f"Spec entry #{idx} missing required string 'name'.")
        if not cwd or not isinstance(cwd, str):
            raise ValueError(f"Spec entry #{idx} ({name!r}) missing required string 'cwd'.")

        sandbox = raw.get("sandbox", "workspace-write")
        if sandbox not in VALID_SANDBOXES:
            raise ValueError(f"Spec entry {name!r} has invalid sandbox {sandbox!r}.")
        approval_policy = raw.get("approval_policy", "on-request")
        if approval_policy not in VALID_APPROVAL_POLICIES:
            raise ValueError(f"Spec entry {name!r} has invalid approval_policy {approval_policy!r}.")
        reviewer = raw.get("approvals_reviewer", "auto_review")
        if reviewer not in VALID_REVIEWERS:
            raise ValueError(f"Spec entry {name!r} has invalid approvals_reviewer {reviewer!r}.")

        is_readonly = sandbox == "read-only" or bool(raw.get("read_only"))
        wt = raw.get("worktree")
        if wt and not is_readonly:
            wt_norm = str(Path(wt).resolve())
            if wt_norm in seen_worktrees:
                raise ValueError(
                    f"Worktree isolation collision: {wt_norm!r} assigned to both "
                    f"{seen_worktrees[wt_norm]!r} and {name!r}."
                )
            seen_worktrees[wt_norm] = name

        out_dir = raw.get("output_dir")
        if out_dir:
            out_norm = str(Path(out_dir).resolve())
            if out_norm in seen_outdirs:
                raise ValueError(
                    f"Output directory isolation collision: {out_norm!r} assigned to both "
                    f"{seen_outdirs[out_norm]!r} and {name!r}."
                )
            seen_outdirs[out_norm] = name

        prompt = raw.get("prompt")
        prompt_file = raw.get("prompt_file")
        if prompt is not None and prompt_file is not None:
            raise ValueError(f"Spec entry {name!r} cannot set both 'prompt' and 'prompt_file'.")
        if prompt_file is not None:
            pf = Path(prompt_file)
            if not pf.is_absolute():
                pf = (spec_dir / pf).resolve()
            if not pf.exists():
                raise FileNotFoundError(f"Spec entry {name!r} prompt_file not found: {pf}")
            prompt = pf.read_text(encoding="utf-8")

        item = dict(raw)
        item["resolved_prompt"] = prompt
        item["sandbox"] = sandbox
        item["approval_policy"] = approval_policy
        item["approvals_reviewer"] = reviewer
        normalized.append(item)

    return normalized


def _read_text_arg(message: str | None, message_file: str | None) -> str:
    if message is not None and message_file is not None:
        raise ValueError("Provide either --message or --message-file, not both.")
    if message_file is not None:
        if message_file == "-":
            return sys.stdin.read()
        return Path(message_file).read_text(encoding="utf-8")
    if message is not None:
        if message == "-":
            return sys.stdin.read()
        return message
    raise ValueError("One of --message or --message-file is required.")


def _load_ids_from_registry(registry_path: str) -> list[str]:
    data = json.loads(Path(registry_path).read_text(encoding="utf-8"))
    sessions = data.get("sessions") if isinstance(data, dict) else data
    if not isinstance(sessions, list):
        raise ValueError(f"Invalid fleet registry format in {registry_path}")
    ids = []
    for item in sessions:
        if isinstance(item, dict):
            tid = item.get("id") or item.get("thread_id") or item.get("threadId")
            if tid:
                ids.append(str(tid))
    return ids


def _print_json(obj: Any, *, pretty: bool = False) -> None:
    if pretty:
        print(json.dumps(obj, indent=2, ensure_ascii=False), flush=True)
    else:
        print(json.dumps(obj, ensure_ascii=False), flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="codex_app_ctl.py",
        description="Official Codex app-server WebSocket JSON-RPC fleet & session controller.",
    )
    p.add_argument(
        "--socket",
        default=None,
        help="Explicit Unix socket path (default: auto-discover via `codex app-server daemon version`).",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Socket timeout in seconds (default: 20).",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="Pretty-print JSON output.",
    )

    # Allow global transport/formatting flags both before and after the subcommand
    # without letting subparser defaults clobber values parsed at top level.
    sub_common = argparse.ArgumentParser(add_help=False)
    sub_common.add_argument(
        "--socket",
        default=argparse.SUPPRESS,
        help="Explicit Unix socket path (default: auto-discover via `codex app-server daemon version`).",
    )
    sub_common.add_argument(
        "--timeout",
        type=float,
        default=argparse.SUPPRESS,
        help="Socket timeout in seconds (default: 20).",
    )
    sub_common.add_argument(
        "--pretty",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Pretty-print JSON output.",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # health
    sub.add_parser(
        "health",
        parents=[sub_common],
        help="Check daemon connectivity, defaults, quota health, and loaded threads.",
    )

    # list
    s_list = sub.add_parser(
        "list",
        parents=[sub_common],
        help="List Codex sessions visible in the app-server.",
    )
    s_list.add_argument("--search", default=None, help="Filter threads by search term.")
    s_list.add_argument("--cwd", default=None, help="Filter threads by working directory.")
    s_list.add_argument("--limit", type=int, default=25, help="Maximum number of threads to return.")
    s_list.add_argument("--archived", action="store_true", default=None, help="Include archived threads.")
    s_list.add_argument("--loaded-only", action="store_true", help="Only show currently loaded threads.")
    s_list.add_argument(
        "--scan-rollouts",
        action="store_true",
        help="Scan JSONL rollouts on disk instead of state-DB-only listing (slower on large histories).",
    )

    # status
    s_status = sub.add_parser(
        "status",
        parents=[sub_common],
        help="Inspect live status and latest turn of one or more threads.",
    )
    s_status.add_argument("ids", nargs="*", help="Thread UUID(s).")
    s_status.add_argument("--registry", default=None, help="Load thread IDs from a FLEET-REGISTRY.json file.")
    s_status.add_argument(
        "--verify-policy",
        action="store_true",
        help="Read rollout JSONL to verify ground-truth `turn_context` policy/sandbox/model.",
    )
    s_status.add_argument(
        "--last-message",
        action="store_true",
        help="Include a preview of the latest assistant message.",
    )

    # create
    s_create = sub.add_parser(
        "create",
        parents=[sub_common],
        help="Create a persistent UI-visible Codex session.",
    )
    s_create.add_argument("--name", required=True, help="Human-readable session name shown in the Codex UI.")
    s_create.add_argument("--cwd", default=os.getcwd(), help="Working directory for the session.")
    s_create.add_argument("--prompt", default=None, help="Initial Turn-1 prompt text (or '-' for stdin).")
    s_create.add_argument("--prompt-file", default=None, help="File containing initial Turn-1 prompt.")
    s_create.add_argument("--model", default=None, help="Model override (defaults to ~/.codex/config.toml).")
    s_create.add_argument("--effort", default=None, help="Reasoning effort override (e.g. high, max).")
    s_create.add_argument("--approval-policy", default="on-request", choices=VALID_APPROVAL_POLICIES)
    s_create.add_argument("--approvals-reviewer", default="auto_review", choices=VALID_REVIEWERS)
    s_create.add_argument("--sandbox", default="workspace-write", choices=VALID_SANDBOXES)
    s_create.add_argument("--writable-root", action="append", default=[], help="Extra writable root (repeatable).")
    s_create.add_argument("--network-access", action="store_true", help="Allow network access inside sandbox.")
    s_create.add_argument("--developer-instructions", default=None, help="Optional thread-level developer instructions.")
    s_create.add_argument("--ephemeral", action="store_true", help="Create an ephemeral thread (not persisted).")
    s_create.add_argument("--check-quota", action="store_true", help="Abort before creation if Codex quota is exhausted.")

    # launch-batch
    s_batch = sub.add_parser(
        "launch-batch",
        parents=[sub_common],
        help="Validate and launch a multi-session fleet from a JSON spec.",
    )
    s_batch.add_argument("--spec", required=True, help="Path to fleet spec JSON file.")
    s_batch.add_argument("--registry-out", default=None, help="Path to write FLEET-REGISTRY.json.")
    s_batch.add_argument("--dry-run", action="store_true", help="Validate spec and print plan without RPC writes.")
    s_batch.add_argument("--check-quota", action="store_true", help="Abort if Codex quota is exhausted.")

    # dispatch
    s_disp = sub.add_parser(
        "dispatch",
        parents=[sub_common],
        help="Smart non-blocking dispatch (`turn/steer` if active, `thread/resume` + `turn/start` if unloaded, `turn/start` if idle).",
    )
    s_disp.add_argument("--thread", required=True, help="Target thread UUID.")
    s_disp.add_argument("--message", default=None, help="Directive text (or '-' for stdin).")
    s_disp.add_argument("--message-file", default=None, help="File containing directive text.")
    s_disp.add_argument("--model", default=None, help="Model override for new turns.")
    s_disp.add_argument("--effort", default=None, help="Reasoning effort override for new turns.")
    s_disp.add_argument("--cwd", default=None, help="Working directory override for new turns.")
    s_disp.add_argument("--approval-policy", default="on-request", choices=VALID_APPROVAL_POLICIES)
    s_disp.add_argument("--approvals-reviewer", default="auto_review", choices=VALID_REVIEWERS)
    s_disp.add_argument("--sandbox", default="workspace-write", choices=VALID_SANDBOXES)
    s_disp.add_argument("--writable-root", action="append", default=[], help="Extra writable root (repeatable).")
    s_disp.add_argument("--network-access", action="store_true", help="Allow network access inside sandbox.")
    s_disp.add_argument("--steer-only", action="store_true", help="Only steer an active turn; skip if idle/unloaded.")
    s_disp.add_argument("--queue-only", action="store_true", help="Queue via `codex queue --resume` instead of live RPC.")
    s_disp.add_argument("--check-quota", action="store_true", help="Abort if Codex quota is exhausted.")

    # set-policy
    s_pol = sub.add_parser(
        "set-policy",
        parents=[sub_common],
        help="Apply approval/sandbox/model policy to existing threads.",
    )
    s_pol.add_argument("ids", nargs="+", help="Thread UUID(s).")
    s_pol.add_argument("--model", default=None)
    s_pol.add_argument("--effort", default=None)
    s_pol.add_argument("--approval-policy", default="on-request", choices=VALID_APPROVAL_POLICIES)
    s_pol.add_argument("--approvals-reviewer", default="auto_review", choices=VALID_REVIEWERS)
    s_pol.add_argument("--sandbox", default="workspace-write", choices=VALID_SANDBOXES)

    # interrupt
    s_int = sub.add_parser(
        "interrupt",
        parents=[sub_common],
        help="Interrupt an active turn on a thread.",
    )
    s_int.add_argument("--thread", required=True, help="Thread UUID.")
    s_int.add_argument("--turn", default=None, help="Explicit turn UUID (default: auto-detect active turn).")

    # archive / unarchive / delete
    s_arch = sub.add_parser("archive", parents=[sub_common], help="Archive one or more threads.")
    s_arch.add_argument("ids", nargs="+")

    s_unarch = sub.add_parser("unarchive", parents=[sub_common], help="Unarchive one or more threads.")
    s_unarch.add_argument("ids", nargs="+")

    s_del = sub.add_parser("delete", parents=[sub_common], help="Permanently delete one or more threads.")
    s_del.add_argument("ids", nargs="+")

    # inspect-rollout
    s_roll = sub.add_parser(
        "inspect-rollout",
        parents=[sub_common],
        help="Inspect a thread's rollout JSONL for ground-truth turn_context & errors.",
    )
    s_roll.add_argument("--thread", default=None, help="Thread UUID (resolves rollout path via app-server).")
    s_roll.add_argument("--path", default=None, help="Direct local path to rollout JSONL (no daemon needed).")
    s_roll.add_argument("--last-n", type=int, default=5, help="Number of recent assistant messages/errors to return.")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Offline path for inspect-rollout --path (no daemon required).
    if args.cmd == "inspect-rollout" and args.path and not args.thread:
        _print_json(inspect_rollout_file(args.path, last_n=args.last_n), pretty=args.pretty)
        return 0

    if args.cmd == "launch-batch" and args.dry_run:
        spec_path = Path(args.spec).resolve()
        raw_specs = json.loads(spec_path.read_text(encoding="utf-8"))
        validated = validate_fleet_spec(raw_specs, spec_path.parent)
        summary = [
            {
                "slot": item.get("slot", i + 1),
                "name": item["name"],
                "existing_thread_id": item.get("existing_thread_id"),
                "cwd": item["cwd"],
                "worktree": item.get("worktree"),
                "branch": item.get("branch"),
                "output_dir": item.get("output_dir"),
                "model": item.get("model"),
                "effort": item.get("effort"),
                "approval_policy": item["approval_policy"],
                "approvals_reviewer": item["approvals_reviewer"],
                "sandbox": item["sandbox"],
                "has_prompt": item["resolved_prompt"] is not None,
            }
            for i, item in enumerate(validated)
        ]
        _print_json({"dry_run": True, "count": len(summary), "sessions": summary}, pretty=args.pretty)
        return 0

    try:
        with CodexAppServerClient(sock_path=args.socket, timeout=args.timeout) as cli:
            if args.cmd == "health":
                _print_json(cli.health(), pretty=args.pretty)

            elif args.cmd == "list":
                rows = cli.list_threads(
                    search=args.search,
                    cwd=args.cwd,
                    limit=args.limit,
                    archived=args.archived,
                    loaded_only=args.loaded_only,
                    use_state_db_only=not args.scan_rollouts,
                )
                _print_json({"count": len(rows), "threads": rows}, pretty=args.pretty)

            elif args.cmd == "status":
                ids = list(args.ids)
                if args.registry:
                    ids.extend(_load_ids_from_registry(args.registry))
                if not ids:
                    parser.error("status requires at least one thread ID or --registry.")
                for tid in ids:
                    res = cli.thread_status(
                        tid,
                        verify_policy=args.verify_policy,
                        include_last_message=args.last_message,
                    )
                    _print_json(res, pretty=args.pretty)

            elif args.cmd == "create":
                if args.check_quota:
                    cli.check_quota_or_raise()
                prompt = None
                if args.prompt is not None or args.prompt_file is not None:
                    prompt = _read_text_arg(args.prompt, args.prompt_file)
                res = cli.start_thread(
                    name=args.name,
                    cwd=args.cwd,
                    prompt=prompt,
                    model=args.model,
                    effort=args.effort,
                    approval_policy=args.approval_policy,
                    approvals_reviewer=args.approvals_reviewer,
                    sandbox=args.sandbox,
                    writable_roots=args.writable_root,
                    network_access=args.network_access,
                    developer_instructions=args.developer_instructions,
                    ephemeral=args.ephemeral,
                )
                _print_json(res, pretty=args.pretty)

            elif args.cmd == "launch-batch":
                if args.check_quota:
                    cli.check_quota_or_raise()
                spec_path = Path(args.spec).resolve()
                raw_specs = json.loads(spec_path.read_text(encoding="utf-8"))
                validated = validate_fleet_spec(raw_specs, spec_path.parent)
                results = []
                for idx, item in enumerate(validated):
                    existing_tid = item.get("existing_thread_id")
                    if existing_tid:
                        if item["resolved_prompt"] is not None:
                            disp = cli.dispatch(
                                existing_tid,
                                item["resolved_prompt"],
                                model=item.get("model"),
                                effort=item.get("effort"),
                                approval_policy=item["approval_policy"],
                                approvals_reviewer=item["approvals_reviewer"],
                                sandbox=item["sandbox"],
                                writable_roots=item.get("writable_roots"),
                                network_access=bool(item.get("network_access", False)),
                                cwd=item["cwd"],
                            )
                        else:
                            disp = {"mode": "reused_only", "threadId": existing_tid}
                        info = cli.read_thread(existing_tid, include_turns=False)
                        entry = {
                            "slot": item.get("slot", idx + 1),
                            "id": existing_tid,
                            "name": info.get("name") or item["name"],
                            "role": item.get("role"),
                            "cwd": info.get("cwd") or item["cwd"],
                            "worktree": item.get("worktree"),
                            "branch": item.get("branch"),
                            "output_dir": item.get("output_dir"),
                            "model": item.get("model") or info.get("model"),
                            "effort": item.get("effort") or info.get("reasoningEffort"),
                            "approval_policy": item["approval_policy"],
                            "approvals_reviewer": item["approvals_reviewer"],
                            "sandbox": item["sandbox"],
                            "cluster_write_owner": bool(item.get("cluster_write_owner", False)),
                            "dispatch": disp,
                        }
                    else:
                        started = cli.start_thread(
                            name=item["name"],
                            cwd=item["cwd"],
                            prompt=item["resolved_prompt"],
                            model=item.get("model"),
                            effort=item.get("effort"),
                            approval_policy=item["approval_policy"],
                            approvals_reviewer=item["approvals_reviewer"],
                            sandbox=item["sandbox"],
                            writable_roots=item.get("writable_roots"),
                            network_access=bool(item.get("network_access", False)),
                            developer_instructions=item.get("developer_instructions"),
                            ephemeral=bool(item.get("ephemeral", False)),
                        )
                        entry = {
                            "slot": item.get("slot", idx + 1),
                            "id": started["id"],
                            "name": started["name"],
                            "role": item.get("role"),
                            "cwd": started["cwd"],
                            "worktree": item.get("worktree"),
                            "branch": item.get("branch"),
                            "output_dir": item.get("output_dir"),
                            "model": started["model"],
                            "effort": started["reasoningEffort"],
                            "approval_policy": started["approvalPolicy"],
                            "approvals_reviewer": started["approvalsReviewer"],
                            "sandbox": started["sandbox"],
                            "turn_id": started["turn_id"],
                            "cluster_write_owner": bool(item.get("cluster_write_owner", False)),
                        }
                    results.append(entry)
                    _print_json(entry, pretty=args.pretty)

                registry_path = (
                    Path(args.registry_out)
                    if args.registry_out
                    else spec_path.with_suffix(".launched.json")
                )
                registry_payload = {
                    "updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "socket_path": cli.sock_path,
                    "session_count": len(results),
                    "sessions": results,
                }
                registry_path.parent.mkdir(parents=True, exist_ok=True)
                registry_path.write_text(
                    json.dumps(registry_payload, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )

            elif args.cmd == "dispatch":
                msg = _read_text_arg(args.message, args.message_file)
                res = cli.dispatch(
                    args.thread,
                    msg,
                    model=args.model,
                    effort=args.effort,
                    approval_policy=args.approval_policy,
                    approvals_reviewer=args.approvals_reviewer,
                    sandbox=args.sandbox,
                    writable_roots=args.writable_root,
                    network_access=args.network_access,
                    cwd=args.cwd,
                    steer_only=args.steer_only,
                    queue_only=args.queue_only,
                    check_quota=args.check_quota,
                )
                _print_json(res, pretty=args.pretty)

            elif args.cmd == "set-policy":
                for tid in args.ids:
                    res = cli.apply_policy(
                        tid,
                        model=args.model,
                        effort=args.effort,
                        approval_policy=args.approval_policy,
                        approvals_reviewer=args.approvals_reviewer,
                        sandbox=args.sandbox,
                    )
                    _print_json(res, pretty=args.pretty)

            elif args.cmd == "interrupt":
                _print_json(cli.interrupt_turn(args.thread, args.turn), pretty=args.pretty)

            elif args.cmd == "archive":
                for tid in args.ids:
                    _print_json(cli.archive_thread(tid), pretty=args.pretty)

            elif args.cmd == "unarchive":
                for tid in args.ids:
                    _print_json(cli.unarchive_thread(tid), pretty=args.pretty)

            elif args.cmd == "delete":
                for tid in args.ids:
                    _print_json(cli.delete_thread(tid), pretty=args.pretty)

            elif args.cmd == "inspect-rollout":
                if not args.thread:
                    parser.error("inspect-rollout requires --thread or --path.")
                th = cli.read_thread(args.thread, include_turns=False)
                rpath = th.get("path")
                if not rpath:
                    raise FileNotFoundError(f"Thread {args.thread} has no rollout path reported.")
                _print_json(inspect_rollout_file(rpath, last_n=args.last_n), pretty=args.pretty)

    except QuotaExhaustedError as exc:
        _print_json(
            {"ok": False, "error": "quota_exhausted", "detail": str(exc), "rateLimits": exc.rate_limits},
            pretty=args.pretty,
        )
        return 3
    except (CodexRPCError, FileNotFoundError, RuntimeError, ValueError, EOFError, OSError) as exc:
        err_payload: dict[str, Any] = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}
        if isinstance(exc, CodexRPCError):
            err_payload["method"] = exc.method
            err_payload["code"] = exc.code
            err_payload["data"] = exc.data
        _print_json(err_payload, pretty=args.pretty)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
