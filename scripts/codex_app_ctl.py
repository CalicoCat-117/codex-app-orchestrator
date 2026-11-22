#!/usr/bin/env python3
"""Zero-dependency official Codex app-server WebSocket JSON-RPC controller.

Orchestrates persistent, UI-visible Codex sessions via the local app-server
daemon's Unix-domain WebSocket control socket. Supports fleet creation,
non-blocking dispatch (`turn/steer` for active turns, `thread/resume` +
`turn/start` for unloaded threads, `turn/start` for idle threads,
`turn/interrupt` + `turn/start` for turns blocked on `request_user_input`),
detection of threads waiting on a question or decision (`status`, `watch`), Guardian
`auto_review` policy management, rollout policy verification, quota health
checks, and lifecycle teardown (`retire` archives sessions with no follow-up;
`dispatch` unarchives an archived thread before resuming it).
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Iterable, Iterator

CLIENT_INFO = {
    "name": "codex-app-orchestrator",
    "title": "Codex App Orchestrator",
    "version": "1.0.0",
}

VALID_APPROVAL_POLICIES = ("on-request", "untrusted", "never")
VALID_REVIEWERS = ("auto_review", "user")
VALID_SANDBOXES = ("workspace-write", "read-only", "danger-full-access")

DEFAULT_CAPTION_GUIDE = (
    "End each substantive turn with a short, plain `Caption:` section (3-5 sentences or tight bullets) "
    "that is easy to read and free of decorative formatting:\n"
    "- Explain the underlying mechanism, root cause, or design rationale rather than only listing surface status.\n"
    "- State concrete observed outcomes (files, commits, measurements, or remaining unknowns) in plain language.\n"
    "- Avoid decorative emojis, hype phrasing, and repetitive compliance or gatekeeping checklists."
)

# Attached to new sessions by default so executors decide reversible choices
# themselves and ask only through one parseable block (see classify_attention).
DEFAULT_AUTONOMY_GUARD = (
    "Questions in supervised autonomous work:\n"
    "1. Inside the task's stated scope and budget, do not stop to ask for confirmation on reversible "
    "choices (parameters, tools, debugging approach, script fixes, naming, comparison baselines). Pick the "
    "most conservative reasonable option, record it and the main alternative you rejected in your report, "
    "and keep going.\n"
    "2. Stop to ask only when the next step would be irreversible or destructive (deleting data, "
    "force-pushing, publishing or sending anything externally), go beyond the stated scope, budget, or "
    "compute limits, use data or credentials not already authorized, or change a frozen goal or headline "
    "conclusion.\n"
    "3. Before stopping, finish all work that does not depend on the answer and release idle resources. "
    "Then end the turn with exactly one block:\n"
    "[DECISION-NEEDED]\n"
    "Question: <one sentence>\n"
    "Options: A) ... | B) ...\n"
    "Recommended: <option> - <one-sentence reason>\n"
    "4. Do not call request_user_input, and do not end a turn on an open question in prose (for example "
    "\"Should I proceed?\"). Ask only through the block above."
)

_CAPTION_HEADING_RE = re.compile(
    r"(?im)^[ \t]*(?:"
    r"#{1,6}[ \t]+(?:\*\*)?caption(?:\*\*)?[ \t]*(?:[:：](?:\*\*)?[ \t]*(.*))?"
    r"|(?:\*\*)?caption(?:\*\*)?[ \t]*[:：](?:\*\*)?[ \t]*(.*)"
    r")$"
)
_NEXT_HEADING_RE = re.compile(r"(?m)^[ \t]*#{1,6}[ \t]+\S")
_LEADING_DECORATIVE_EMOJI_RE = re.compile(
    r"^([ \t]*(?:[-*][ \t]+)?)[✅❌⚠️🚀🔥🎉✨💡📌🟢🔴🟡☑️✔️✖️ℹ️🎯📊📈📉🔧🛠️⚡🌟💥🏆]+[ \t]*"
)


def compose_developer_instructions(
    developer_instructions: str | None,
    *,
    caption_guide: bool = True,
    autonomy_guard: bool = True,
) -> str | None:
    """Attach the autonomy guard and plain-caption guide unless disabled or already present.

    Instructions that already define a `[DECISION-NEEDED...]` block (for example
    tri-autoresearch's tiered guard) keep their own wording.
    """
    base = (developer_instructions or "").strip()
    parts = [base] if base else []
    if autonomy_guard and "[DECISION-NEEDED" not in base:
        parts.append(DEFAULT_AUTONOMY_GUARD)
    if caption_guide and not ("Caption:" in base and "underlying mechanism" in base):
        parts.append(DEFAULT_CAPTION_GUIDE)
    return "\n\n".join(parts) or None


_DECISION_BLOCK_RE = re.compile(r"\[DECISION-NEEDED(?:\s*:\s*([^\]]*))?\]", re.IGNORECASE)
_DECISION_FIELD_RE = re.compile(r"(?im)^[ \t>*-]*(question|options|recommended)[ \t]*[:：][ \t]*(.+)$")
_PROSE_QUESTION_RE = re.compile(
    r"(?:please\s+confirm|which\s+option|would\s+you\s+like\s+me\s+to|should\s+i\s+(?:proceed|continue|go\s+ahead)"
    r"|shall\s+i\b|before\s+i\s+proceed|do\s+you\s+want\s+me\s+to"
    r"|awaiting\s+(?:your|user|human)\s+(?:decision|approval|confirmation|input)"
    r"|let\s+me\s+know\s+(?:which|whether|if\s+you)"
    r"|请确认|请选择|是否继续|是否需要我|要我[^。\n]{0,24}吗|需要(?:你|您|用户|人工)[^。\n]{0,8}(?:决定|确认|选择)"
    r"|等待(?:你|您|用户)?的?(?:确认|决定|批准)|由\s*[^\s，。,]{1,16}\s*(?:选择|决定|拍板|复核|确认))",
    re.IGNORECASE,
)


def parse_decision_block(message: str | None) -> dict[str, Any] | None:
    """Parse the last `[DECISION-NEEDED]` block in an assistant message."""
    if not message:
        return None
    matches = list(_DECISION_BLOCK_RE.finditer(message))
    if not matches:
        return None
    last = matches[-1]
    block: dict[str, Any] = {"raw": message[last.start() :].strip()[:1500]}
    qualifier = (last.group(1) or "").strip()
    tier = re.search(r"tier\s*=\s*([A-Za-z_]+)", qualifier)
    if tier:
        block["tier"] = tier.group(1).lower()
    for m in _DECISION_FIELD_RE.finditer(message[last.end() :]):
        block.setdefault(m.group(1).lower(), m.group(2).strip())
    return block


def _ends_on_prose_question(message: str | None) -> bool:
    """True when the closing lines of a message ask the reader something."""
    if not message:
        return False
    lines = [ln.strip() for ln in message.strip().splitlines() if ln.strip()]
    if not lines:
        return False
    tail = "\n".join(lines[-3:])[-600:]
    return bool(_PROSE_QUESTION_RE.search(tail)) or lines[-1].rstrip("*_` ").endswith(("?", "？"))


def classify_attention(
    status_obj: Any,
    last_turn_status: str | None,
    last_message: str | None,
    *,
    pending_questions: list[Any] | None = None,
) -> dict[str, Any] | None:
    """Return why a thread is waiting on a supervisor, or None when it is not.

    Kinds, strongest first: `waiting_on_user_input` / `waiting_on_approval`
    (protocol-level, the turn cannot continue), `decision_needed` (turn ended on a
    `[DECISION-NEEDED]` block), and `question` (turn ended on a prose question;
    heuristic).
    """
    flags = status_obj.get("activeFlags") if isinstance(status_obj, dict) else None
    flags = flags if isinstance(flags, list) else []
    if "waitingOnUserInput" in flags:
        return {"kind": "waiting_on_user_input", "questions": pending_questions or []}
    if "waitingOnApproval" in flags:
        return {"kind": "waiting_on_approval"}
    if last_turn_status in (None, "inProgress"):
        return None
    block = parse_decision_block(last_message)
    if block:
        return {"kind": "decision_needed", **block}
    if _ends_on_prose_question(last_message):
        tail = (last_message or "").strip()[-400:]
        return {"kind": "question", "heuristic": True, "tail": tail}
    return None


class RolloutReadError(RuntimeError):
    """A rollout exists but cannot be read (for example, it is zstd-compressed and `zstd` is missing)."""


def _rollout_source(path: str | Path) -> Path | None:
    """The file that holds a rollout: `path` itself, or the `<path>.zst` Codex compresses it into.

    `thread/read` and `thread/list` keep reporting the `.jsonl` path after compression.
    """
    p = Path(path)
    if p.exists():
        return p
    compressed = Path(f"{p}.zst")
    return compressed if compressed.exists() else None


# Rollout lines holding tool output can run to megabytes; a larger buffer cuts read calls.
_ROLLOUT_READ_BUFFER = 1 << 20


@contextlib.contextmanager
def _rollout_lines(src: Path) -> Iterator[Iterable[bytes]]:
    """Yield a rollout's raw lines, streaming `.zst` files through the `zstd` CLI (no Python deps)."""
    if src.suffix != ".zst":
        with src.open("rb", buffering=_ROLLOUT_READ_BUFFER) as fh:
            yield fh
        return
    zstd = shutil.which("zstd")
    if not zstd:
        raise RolloutReadError(
            f"Rollout {src} is zstd-compressed and the `zstd` command is not on PATH. Install zstd "
            "(for example `apt install zstd` or `brew install zstd`) to read compressed rollouts."
        )
    proc = subprocess.Popen(
        [zstd, "-d", "-c", "-q", "--", str(src)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=_ROLLOUT_READ_BUFFER,
    )
    read_to_end = False
    try:
        yield proc.stdout
        read_to_end = proc.stdout.read(1) == b""
    finally:
        if not read_to_end:
            # The reader stopped early or failed; zstd may be blocked on a full pipe.
            proc.kill()
        proc.stdout.close()
        err = proc.stderr.read().decode("utf-8", errors="replace").strip()
        proc.stderr.close()
        returncode = proc.wait()
    if read_to_end and returncode != 0:
        raise RolloutReadError(f"zstd could not decompress {src} (exit {returncode}): {err or 'no message'}")


def _rollout_tail_lines(src: Path, tail_bytes: int) -> list[str]:
    """Decoded lines from the last `tail_bytes` of a rollout; a compressed one is streamed to its end."""
    if src.suffix != ".zst":
        size = src.stat().st_size
        with src.open("rb") as fh:
            if size > tail_bytes:
                fh.seek(size - tail_bytes)
            return fh.read().decode("utf-8", errors="ignore").splitlines()
    window: deque[bytes] = deque()
    kept = 0
    with _rollout_lines(src) as lines:
        for raw in lines:
            window.append(raw)
            kept += len(raw)
            while kept > tail_bytes and len(window) > 1:
                kept -= len(window.popleft())
    return [raw.decode("utf-8", errors="ignore").rstrip("\r\n") for raw in window]


def pending_user_input_questions(rollout_path: str | None, tail_bytes: int = 65536) -> list[Any]:
    """Questions from the newest `request_user_input` call in a rollout's tail, if any."""
    if not rollout_path:
        return []
    src = _rollout_source(rollout_path)
    if src is None:
        return []
    try:
        lines = _rollout_tail_lines(src, tail_bytes)
    except OSError:
        return []
    for line in reversed(lines):
        if "request_user_input" not in line:
            continue
        try:
            payload = json.loads(line).get("payload") or {}
        except (json.JSONDecodeError, AttributeError):
            continue
        if payload.get("name") != "request_user_input":
            continue
        args = payload.get("arguments")
        try:
            args = json.loads(args) if isinstance(args, str) else args
        except json.JSONDecodeError:
            return [args]
        questions = args.get("questions") if isinstance(args, dict) else None
        return questions if isinstance(questions, list) else [args]
    return []


def _clean_caption_lines(text: str) -> str:
    cleaned_lines = []
    for line in text.splitlines():
        line = _LEADING_DECORATIVE_EMOJI_RE.sub(r"\1", line).rstrip()
        cleaned_lines.append(line)
    joined = "\n".join(cleaned_lines).strip()
    return re.sub(r"\n{3,}", "\n\n", joined)


def _trim_at_boundary(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    window = text[:max_chars]
    min_keep = max(40, max_chars // 2)
    for sep in ("\n\n", "\n", "。", ". ", "! ", "? "):
        idx = window.rfind(sep)
        if idx >= min_keep:
            end = idx + (len(sep) if sep in ("。",) else (1 if sep in (". ", "! ", "? ") else 0))
            trimmed = window[:end].rstrip()
            suffix = " ..." if trimmed.endswith((".", "。", "!", "?")) else "..."
            return trimmed + suffix
    sp = window.rfind(" ")
    if sp >= min_keep:
        return window[:sp].rstrip() + "..."
    return window.rstrip() + "..."


def extract_caption_text(raw_text: str | None, *, max_chars: int = 1200) -> tuple[str, str]:
    """Extract a plain, readable caption from an assistant message.

    Returns `(caption_text, source)` where `source` is `"explicit_caption"`,
    `"latest_message"`, or `"empty"`.
    """
    if not raw_text or not raw_text.strip():
        return ("", "empty")

    matches = list(_CAPTION_HEADING_RE.finditer(raw_text))
    if matches:
        last = matches[-1]
        inline = (last.group(1) or last.group(2) or "").strip()
        tail = raw_text[last.end() :]
        next_hdr = _NEXT_HEADING_RE.search(tail)
        if next_hdr:
            tail = tail[: next_hdr.start()]
        body = f"{inline}\n{tail}".strip() if inline else tail.strip()
        cleaned = _clean_caption_lines(body)
        if cleaned:
            return (_trim_at_boundary(cleaned, max_chars), "explicit_caption")

    cleaned = _clean_caption_lines(raw_text)
    if not cleaned:
        return ("", "empty")
    return (_trim_at_boundary(cleaned, max_chars), "latest_message")


def _extract_user_prompt_from_turn(turn: dict[str, Any] | None) -> str | None:
    """Extract a short preview of the user input from an active turn."""
    if not isinstance(turn, dict):
        return None
    for item in turn.get("items") or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") in ("userMessage", "user"):
            if isinstance(item.get("text"), str) and item["text"].strip():
                return item["text"].strip()
            content = item.get("content")
            if isinstance(content, list):
                parts = [
                    p.get("text", "")
                    for p in content
                    if isinstance(p, dict) and isinstance(p.get("text"), str)
                ]
                joined = "\n".join(x for x in parts if x).strip()
                if joined:
                    return joined
    return None


def _summarize_policy(turn_ctx: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(turn_ctx, dict):
        return None
    sb = turn_ctx.get("sandbox_policy")
    sb_type = sb.get("type") if isinstance(sb, dict) else sb
    out = {
        "approval_policy": turn_ctx.get("approval_policy"),
        "approvals_reviewer": turn_ctx.get("approvals_reviewer"),
        "sandbox": sb_type,
    }
    return out if any(v is not None for v in out.values()) else None


def _errors_for_turn(errors: list[dict[str, Any]] | None, turn_id: str | None) -> list[dict[str, Any]]:
    """Keep only rollout errors from `turn_id`, so an earlier turn's abort is not reported as current."""
    if not errors:
        return []
    if not turn_id:
        return list(errors)
    return [e for e in errors if not isinstance(e, dict) or e.get("turn_id") in (None, turn_id)]


def _format_error_summary(last_turn_error: Any, recent_errors: list[dict[str, Any]] | None) -> str | None:
    if last_turn_error:
        if isinstance(last_turn_error, dict):
            return str(last_turn_error.get("message") or last_turn_error.get("code") or last_turn_error)
        return str(last_turn_error)
    if recent_errors:
        latest = recent_errors[-1]
        msg = latest.get("message") if isinstance(latest, dict) else latest
        ev_type = latest.get("type") if isinstance(latest, dict) else None
        if msg:
            return f"{ev_type}: {msg}" if ev_type else str(msg)
    return None


def format_session_caption(record: dict[str, Any], *, show_policy: bool = False) -> str:
    """Render a plain, readable, unembellished caption block for one session."""
    name = (record.get("name") or record.get("id") or "Session").strip()
    slot = record.get("slot")
    if slot is not None and not re.match(r"(?i)^slot\s+\d+", name):
        title = f"Slot {slot} · {name}"
    else:
        title = name

    status_type = record.get("status_type") or "unknown"
    lt_status = record.get("last_turn_status")
    if status_type == "active" and lt_status == "inProgress":
        state_str = "active (turn in progress)"
    elif lt_status:
        state_str = f"{status_type} (last turn {lt_status})"
    else:
        state_str = str(status_type)

    lines = [f"{title} — {state_str}"]

    ctx_parts = []
    cwd = record.get("cwd")
    branch = record.get("branch")
    if cwd:
        ctx_parts.append(f"Workspace: {cwd} ({branch})" if branch else f"Workspace: {cwd}")
    elif branch:
        ctx_parts.append(f"Branch: {branch}")

    model = record.get("model")
    effort = record.get("effort")
    if model:
        ctx_parts.append(f"Model: {model} ({effort})" if effort else f"Model: {model}")
    if ctx_parts:
        lines.append(" | ".join(ctx_parts))

    if show_policy:
        pol = record.get("policy")
        if isinstance(pol, dict):
            pol_parts = [str(pol[k]) for k in ("approval_policy", "approvals_reviewer", "sandbox") if pol.get(k)]
            if pol_parts:
                lines.append("Policy: " + " / ".join(pol_parts))

    if record.get("error"):
        lines.append(f"Note: {record['error']}")

    caption_text = (record.get("caption") or "").strip()
    lines.append("Caption:")
    if caption_text:
        for cl in caption_text.splitlines():
            lines.append(f"  {cl}" if cl.strip() else "")
    else:
        lines.append("  No assistant summary recorded yet.")

    return "\n".join(lines)


def rollout_caption(
    path: str | Path,
    *,
    name: str | None = None,
    max_chars: int = 1200,
) -> dict[str, Any]:
    """Build a structured caption record directly from a local rollout JSONL file (offline)."""
    rollout = inspect_rollout_file(path, last_n=3, include_full_last_message=True)
    if not rollout.get("exists"):
        raise FileNotFoundError(f"Rollout file not found: {path}")
    ctx = rollout.get("last_turn_context") or {}
    last_turn_id = rollout.get("last_turn_id") or ctx.get("turn_id")
    caption_text, source = extract_caption_text(rollout.get("last_assistant_text"), max_chars=max_chars)
    p = Path(path)
    return {
        "id": None,
        "slot": None,
        "name": name or Path(p.name[: -len(".zst")] if p.suffix == ".zst" else p.name).stem,
        # Read from the file alone, so the live thread state is unknown.
        "status_type": "offline",
        "last_turn_id": last_turn_id,
        "last_turn_status": rollout.get("last_turn_status"),
        "cwd": ctx.get("cwd"),
        "branch": None,
        "model": ctx.get("model"),
        "effort": ctx.get("effort"),
        "caption": caption_text,
        "caption_source": source,
        "error": _format_error_summary(None, _errors_for_turn(rollout.get("recent_errors"), last_turn_id)),
        "policy": _summarize_policy(ctx),
        "path": str(p),
    }


_MODEL_ID_RE = re.compile(r"^(?P<prefix>.+?)-(?P<version>\d+(?:\.\d+)*)-(?P<family>[a-z][a-z0-9]*)$")


def _parse_model_id(model_id: Any) -> tuple[str, tuple[int, ...], str] | None:
    """Split ids like `gpt-6.2-sol` into ("gpt", (6, 2), "sol"); None for other shapes."""
    m = _MODEL_ID_RE.match(model_id) if isinstance(model_id, str) else None
    if not m:
        return None
    return m["prefix"], tuple(int(x) for x in m["version"].split(".")), m["family"]


def newest_in_family(model_ids: list[str], reference: str | None) -> str | None:
    """Newest id sharing `reference`'s prefix and family suffix (e.g. the latest `gpt-*-sol`)."""
    ref = _parse_model_id(reference)
    if not ref:
        return None
    best: tuple[tuple[int, ...], str] | None = None
    for mid in model_ids:
        parsed = _parse_model_id(mid)
        if parsed and parsed[0] == ref[0] and parsed[2] == ref[2]:
            if best is None or parsed[1] > best[0]:
                best = (parsed[1], mid)
    return best[1] if best else None


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


# Every record inspect_rollout_file uses carries one of these JSON strings, so other
# lines (mostly large tool output) are skipped before JSON parsing.
_ROLLOUT_LINE_HINT_RE = re.compile(
    rb'"(?:turn_context|assistant|task_started|task_complete|error|turn_aborted|warning)"'
)


def inspect_rollout_file(
    path: str | Path,
    last_n: int = 5,
    *,
    include_full_last_message: bool = False,
) -> dict[str, Any]:
    """Read-only local inspection of a Codex rollout JSONL file.

    Extracts the latest `turn_context` (ground-truth effective approval policy,
    approvals reviewer, sandbox policy, model, effort), recent assistant messages,
    any error/rate-limit events (tagged with the turn they belong to), and the
    id and terminal state of the most recent turn (`last_turn_status`:
    `inProgress`, `completed`, `interrupted`, or `aborted`).

    Reads `<path>.zst` when Codex has compressed the rollout (`compressed: true`;
    `size_bytes` is then the compressed size). Raises RolloutReadError when that
    copy cannot be decompressed, so a compressed rollout never reads as missing.
    """
    p = Path(path)
    src = _rollout_source(p)
    if src is None:
        return {"exists": False, "path": str(p)}
    size_bytes = src.stat().st_size

    last_turn_context: dict[str, Any] | None = None
    turn_context_count = 0
    assistant_messages: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    task_completes = 0
    # Follow the most recent turn in file order. A turn opens with `task_started`
    # (newer rollouts) or its first `turn_context` (older ones) and closes with
    # `task_complete` or `turn_aborted`; events without a turn_id apply to the open turn.
    current_turn_id: str | None = None
    current_turn_status: str | None = None

    with _rollout_lines(src) as lines:
        for raw in lines:
            if not _ROLLOUT_LINE_HINT_RE.search(raw):
                continue
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue

            entry_type = obj.get("type")
            payload = obj.get("payload")
            if entry_type == "turn_context" and isinstance(payload, dict):
                turn_context_count += 1
                ctx_turn_id = payload.get("turn_id")
                opens_new_turn = (
                    current_turn_status is None
                    or (ctx_turn_id and ctx_turn_id != current_turn_id)
                    # Old rollouts without turn ids: a context after a closed turn starts the next one.
                    or (not ctx_turn_id and current_turn_status != "inProgress")
                )
                if opens_new_turn:
                    current_turn_id = ctx_turn_id
                    current_turn_status = "inProgress"
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
                ev_turn_id = payload.get("turn_id")
                applies_to_current = ev_turn_id is None or ev_turn_id == current_turn_id
                if ev_type == "task_started":
                    current_turn_id = ev_turn_id
                    current_turn_status = "inProgress"
                elif ev_type == "task_complete":
                    task_completes += 1
                    if applies_to_current:
                        current_turn_status = "completed"
                if ev_type in ("error", "turn_aborted", "warning"):
                    errors.append(
                        {
                            "timestamp": obj.get("timestamp"),
                            "type": ev_type,
                            "turn_id": ev_turn_id or current_turn_id,
                            "message": payload.get("message") or payload.get("reason") or payload,
                        }
                    )
                    if ev_type == "turn_aborted" and applies_to_current:
                        current_turn_status = (
                            "interrupted" if payload.get("reason") == "interrupted" else "aborted"
                        )

    trimmed_messages = [
        {
            "timestamp": m["timestamp"],
            "preview": m["text"][:500],
            "length": len(m["text"]),
        }
        for m in assistant_messages[-max(1, last_n) :]
    ]

    result: dict[str, Any] = {
        "exists": True,
        "path": str(p),
        "compressed": src.suffix == ".zst",
        "size_bytes": size_bytes,
        "turn_context_count": turn_context_count,
        "task_complete_count": task_completes,
        "last_turn_id": current_turn_id,
        "last_turn_status": current_turn_status,
        "last_turn_context": last_turn_context,
        "recent_assistant_messages": trimmed_messages,
        "recent_errors": errors[-max(1, last_n) :],
    }
    if include_full_last_message:
        result["last_assistant_text"] = assistant_messages[-1]["text"] if assistant_messages else None
    return result


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


def _is_archived_error(exc: CodexRPCError) -> bool:
    """`thread/resume` on an archived thread fails with "session <id> is archived"."""
    return "is archived" in (exc.rpc_message or str(exc)).lower()


def _is_active_writer_error(exc: CodexRPCError) -> bool:
    """Another codex process (an open TUI, a plugin's app-server) holds the thread:
    "thread <id> already has an active writer"."""
    return "active writer" in (exc.rpc_message or str(exc)).lower()


def _is_archived_path(path: Any) -> bool:
    """Archiving moves a thread's rollout under ~/.codex/archived_sessions/."""
    return isinstance(path, str) and "/archived_sessions/" in path.replace("\\", "/")


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

    def list_models(self, *, include_hidden: bool = False) -> list[dict[str, Any]]:
        models: list[dict[str, Any]] = []
        cursor = None
        for _ in range(20):
            params: dict[str, Any] = {"includeHidden": include_hidden}
            if cursor:
                params["cursor"] = cursor
            res = self.rpc("model/list", params)
            models.extend(m for m in (res.get("data") or []) if isinstance(m, dict))
            cursor = res.get("nextCursor")
            if not cursor:
                break
        return models

    def model_freshness(self, configured: str | None) -> dict[str, Any]:
        """Compare the configured default model with the newest visible model of its family.

        Model ids look like `<prefix>-<version>-<family>` (`gpt-6.2-sol`), so the
        newest family member is the highest version with the same prefix and suffix.
        """
        try:
            models = self.list_models()
        except CodexRPCError as exc:
            return {"configured": configured, "error": exc.rpc_message}
        ids = [str(m.get("id") or m.get("model")) for m in models if m.get("id") or m.get("model")]
        newest = newest_in_family(ids, configured)
        info: dict[str, Any] = {
            "configured": configured,
            "newestInFamily": newest,
            "configuredIsNewest": (newest == configured) if newest else None,
            "available": ids,
            "warnings": [],
        }
        for m in models:
            if (m.get("id") or m.get("model")) == configured:
                efforts = m.get("supportedReasoningEfforts") or []
                info["supportedEfforts"] = [
                    e.get("reasoningEffort") if isinstance(e, dict) else e for e in efforts
                ]
        if configured and ids and configured not in ids:
            info["warnings"].append(f"Configured model {configured!r} is not offered by this app-server.")
        if newest and newest != configured:
            info["warnings"].append(
                f"A newer {_parse_model_id(newest)[2]} model is available: {newest} (configured: {configured}). "
                "Update `model` in ~/.codex/config.toml so new sessions use it."
            )
        return info

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
        models = self.model_freshness(cfg.get("model"))

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
            "models": models,
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
        """Up to `limit` threads, following `nextCursor` (the server caps a page, at 100 on 0.169)."""
        loaded_set = set(self.list_loaded_threads())
        query: dict[str, Any] = {}
        if search:
            query["searchTerm"] = search
        if cwd:
            query["cwd"] = cwd
        if archived is not None:
            query["archived"] = archived
        state_db_only = use_state_db_only
        out: list[dict[str, Any]] = []
        seen: set[Any] = set()
        cursor: str | None = None
        while len(out) < limit:
            # --loaded-only filters here, after the server's page, so ask for full pages.
            params = dict(query, limit=max(limit, 100) if loaded_only else limit - len(out))
            if cursor:
                params["cursor"] = cursor
            if state_db_only:
                params["useStateDbOnly"] = True
            try:
                res = self.rpc("thread/list", params)
            except CodexRPCError:
                if not state_db_only or cursor:
                    raise
                # Compatibility fallback for older app-server builds without `useStateDbOnly`.
                state_db_only = False
                continue
            rows = res.get("data") or []
            for th in rows:
                tid = th.get("id")
                is_loaded = tid in loaded_set
                if tid in seen or (loaded_only and not is_loaded):
                    continue
                seen.add(tid)
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
                        "archived": _is_archived_path(th.get("path")),
                    }
                )
                if len(out) >= limit:
                    break
            next_cursor = res.get("nextCursor")
            if not rows or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
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
            "archived": _is_archived_path(th.get("path")),
        }
        msg = _extract_agent_message_from_turn(lt)
        msg_source = "turn" if msg is not None else None
        # The turn summary can leave out the agent message (notLoaded threads,
        # compaction turns). When a finished turn's attention or --last-message
        # needs it, take the last assistant text from the rollout instead.
        wants_msg = include_last_message or out["last_turn_status"] not in (None, "inProgress")
        rollout: dict[str, Any] | None = None
        if th.get("path") and (verify_policy or (msg is None and wants_msg)):
            rollout = inspect_rollout_file(th["path"], last_n=3, include_full_last_message=True)
        if msg is None and wants_msg and rollout and rollout.get("last_assistant_text"):
            msg, msg_source = rollout["last_assistant_text"], "rollout"
        flags = status_obj.get("activeFlags") if isinstance(status_obj, dict) else None
        pending = (
            pending_user_input_questions(th.get("path"))
            if isinstance(flags, list) and "waitingOnUserInput" in flags
            else None
        )
        attention = classify_attention(
            status_obj, out["last_turn_status"], msg, pending_questions=pending
        )
        if attention and attention["kind"] in ("decision_needed", "question"):
            attention["message_source"] = msg_source
        out["attention"] = attention
        if include_last_message and msg is not None:
            out["last_agent_message"] = msg[:1000]
        if verify_policy and rollout is not None:
            out["rollout_turn_context"] = rollout.get("last_turn_context")
            out["rollout_recent_errors"] = rollout.get("recent_errors")
        return out

    def thread_caption(
        self,
        tid: str,
        *,
        max_chars: int = 1200,
        registry_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a readable, principle-oriented caption record for a live or persisted thread."""
        th = self.read_thread(tid, include_turns=False)
        lt = self.latest_turn(tid)
        status_obj = th.get("status")
        status_type = status_obj.get("type") if isinstance(status_obj, dict) else status_obj
        lt_status = lt.get("status") if isinstance(lt, dict) else None
        lt_error = lt.get("error") if isinstance(lt, dict) else None

        rollout: dict[str, Any] = {}
        rpath = th.get("path")
        if rpath:
            rollout = inspect_rollout_file(rpath, last_n=3, include_full_last_message=True)

        turn_msg = _extract_agent_message_from_turn(lt)
        rollout_msg = rollout.get("last_assistant_text") if rollout.get("exists") else None
        raw_msg = turn_msg or rollout_msg
        caption_text, source = extract_caption_text(raw_msg, max_chars=max_chars)

        if not caption_text and lt_status == "inProgress":
            prompt_preview = _extract_user_prompt_from_turn(lt)
            if prompt_preview:
                first_line = _trim_at_boundary(_clean_caption_lines(prompt_preview), min(max_chars, 240))
                caption_text = f"Turn in progress. Active directive: {first_line}"
            else:
                caption_text = "Turn in progress (no assistant summary yet)."
            source = "in_progress"

        meta = registry_meta or {}
        ctx = rollout.get("last_turn_context") or {}
        last_turn_id = lt.get("id") if isinstance(lt, dict) else ctx.get("turn_id")
        return {
            "id": th.get("id") or tid,
            "slot": meta.get("slot"),
            "name": th.get("name") or meta.get("name"),
            "status_type": status_type,
            "last_turn_id": last_turn_id,
            "last_turn_status": lt_status,
            "cwd": th.get("cwd") or meta.get("worktree") or meta.get("cwd") or ctx.get("cwd"),
            "branch": meta.get("branch"),
            "model": th.get("model") or ctx.get("model") or meta.get("model"),
            "effort": th.get("reasoningEffort") or ctx.get("effort") or meta.get("effort"),
            "caption": caption_text,
            "caption_source": source,
            "error": _format_error_summary(lt_error, _errors_for_turn(rollout.get("recent_errors"), last_turn_id)),
            "policy": _summarize_policy(ctx),
            "path": rpath,
        }

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
        caption_guide: bool = True,
        autonomy_guard: bool = True,
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
        effective_dev_instructions = compose_developer_instructions(
            developer_instructions,
            caption_guide=caption_guide,
            autonomy_guard=autonomy_guard,
        )
        if effective_dev_instructions:
            start_params["developerInstructions"] = effective_dev_instructions

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

        unarchived = False
        try:
            res = self.rpc("thread/resume", params)
        except CodexRPCError as exc:
            if not _is_archived_error(exc):
                raise
            # An archived thread refuses resume. New work for it means the session
            # is live again, so bring it back to the sidebar and resume.
            self.rpc("thread/unarchive", {"threadId": tid})
            unarchived = True
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
            "unarchived": unarchived,
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
        """Route a message by live state: steer an active turn, resume + start an unloaded thread,
        start a turn on an idle one, and interrupt + restart a turn blocked on request_user_input."""
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
        unarchived = False
        if status_type == "notLoaded":
            if steer_only:
                return {
                    "mode": "skipped",
                    "reason": "thread_not_loaded",
                    "threadId": tid,
                    "status": th.get("status"),
                }
            pol = self.apply_policy(
                tid,
                model=model,
                effort=effort,
                approval_policy=approval_policy,
                approvals_reviewer=approvals_reviewer,
                sandbox=sandbox,
            )
            resumed = True
            unarchived = bool(pol.get("unarchived"))
            th = self.read_thread(tid, include_turns=False)

        lt = self.latest_turn(tid)
        live_flags = (th.get("status") or {}).get("activeFlags") if isinstance(th.get("status"), dict) else None
        waiting_on_input = (
            isinstance(live_flags, list)
            and "waitingOnUserInput" in live_flags
            and bool(lt and lt.get("status") == "inProgress" and lt.get("id"))
        )
        interrupted_turn_id = None
        if waiting_on_input:
            # A steer lands in the turn, but the pending request_user_input call still
            # waits for its own response, so the turn stays blocked. Interrupt it and
            # deliver the answer as a fresh turn (the thread history keeps the context).
            if steer_only:
                return {
                    "mode": "skipped",
                    "reason": "waiting_on_user_input",
                    "threadId": tid,
                    "status": th.get("status"),
                }
            interrupted_turn_id = lt["id"]
            self.interrupt_turn(tid, interrupted_turn_id)
            lt = self._wait_turn_settled(tid, interrupted_turn_id)
            message = (
                "Your previous turn was interrupted while it waited on request_user_input. "
                "Supervisor answer:\n" + message
            )

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
        out = {
            "mode": "interrupt+turn/start" if interrupted_turn_id else "turn/start",
            "resumedFromNotLoaded": resumed,
            "threadId": tid,
            "turnId": (res.get("turn") or {}).get("id"),
        }
        if interrupted_turn_id:
            out["interruptedTurnId"] = interrupted_turn_id
        if unarchived:
            out["unarchived"] = True
        return out

    def _wait_turn_settled(self, tid: str, turn_id: str, timeout_s: float = 15.0) -> dict[str, Any] | None:
        """Poll until `turn_id` is no longer in progress after an interrupt (bounded)."""
        deadline = time.monotonic() + timeout_s
        lt = self.latest_turn(tid)
        while lt and lt.get("id") == turn_id and lt.get("status") == "inProgress":
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Turn {turn_id} was still in progress {timeout_s:.0f}s after turn/interrupt.")
            time.sleep(0.5)
            lt = self.latest_turn(tid)
        return lt

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

    def retire_thread(self, tid: str, *, dry_run: bool = False, force: bool = False) -> dict[str, Any]:
        """Archive a thread only when nothing is left for it to do.

        Skips a running turn (always), and unless `force`: a pending question or
        decision (`attention`), a failed or interrupted last turn, or a thread in
        `systemError`. Those usually mean the session still has work or a reply
        coming. `dispatch` unarchives a thread on demand, so retiring is reversible.
        A thread another codex process holds open cannot be archived; it is
        skipped as `held_by_other_process`. Other RPC errors propagate.
        """
        st = self.thread_status(tid)
        att = st.get("attention") or {}
        rec: dict[str, Any] = {
            "id": st.get("id") or tid,
            "name": st.get("name"),
            "status_type": st.get("status_type"),
            "last_turn_status": st.get("last_turn_status"),
            "attention": att.get("kind"),
        }
        reason = None
        if st.get("archived"):
            reason = "already_archived"
        elif st.get("status_type") == "active" or st.get("last_turn_status") == "inProgress":
            reason = "turn_in_progress"
        elif not force:
            if att:
                reason = f"needs_attention:{att.get('kind')}"
            elif st.get("last_turn_status") in ("failed", "interrupted"):
                reason = f"last_turn_{st['last_turn_status']}"
            elif st.get("status_type") == "systemError":
                reason = "thread_system_error"
        if reason:
            rec.update(action="skipped", reason=reason)
        elif dry_run:
            rec["action"] = "would_archive"
        else:
            try:
                self.archive_thread(tid)
                rec["action"] = "archived"
            except CodexRPCError as exc:
                if not _is_active_writer_error(exc):
                    raise
                rec.update(action="skipped", reason="held_by_other_process", detail=exc.rpc_message)
        return rec

    def unarchive_thread(self, tid: str) -> dict[str, Any]:
        res = self.rpc("thread/unarchive", {"threadId": tid})
        return {"threadId": tid, "unarchived": True, "result": res}

    def delete_thread(self, tid: str) -> dict[str, Any]:
        res = self.rpc("thread/delete", {"threadId": tid})
        return {"threadId": tid, "deleted": True, "result": res}


EXIT_WATCH_TIMEOUT = 4


def _watch_summary(st: dict[str, Any]) -> dict[str, Any]:
    return {
        k: st.get(k)
        for k in ("id", "name", "status_type", "last_turn_id", "last_turn_status", "attention")
    }


def watch_threads(
    make_client: Any,
    ids: list[str],
    *,
    interval: float = 15.0,
    max_wait: float = 3600.0,
    ignore_current: bool = False,
    sleep: Any = time.sleep,
    clock: Any = time.monotonic,
) -> dict[str, Any]:
    """Poll threads until one finishes a turn or needs attention, or `max_wait` passes.

    Each poll opens a fresh connection, so a long watch does not depend on one
    idle socket staying open. Events:
    - `attention`: the thread waits on a question, approval, or decision
      (see classify_attention). Existing attention counts unless `ignore_current`.
    - `turn_finished`: a turn that was running (or a new turn) reached a final state.
    """
    def poll() -> dict[str, dict[str, Any]]:
        with make_client() as cli:
            return {tid: cli.thread_status(tid) for tid in ids}

    start = clock()
    prev = poll()
    polls = 1
    events: list[dict[str, Any]] = []
    if not ignore_current:
        events = [{"type": "attention", **_watch_summary(st)} for st in prev.values() if st.get("attention")]

    while not events:
        remaining = max_wait - (clock() - start)
        if remaining <= 0:
            return {
                "timedOut": True,
                "events": [],
                "polls": polls,
                "threads": [_watch_summary(st) for st in prev.values()],
            }
        sleep(min(interval, remaining))
        cur = poll()
        polls += 1
        for tid, st in cur.items():
            before = prev.get(tid) or {}
            att, att_before = st.get("attention"), before.get("attention")
            new_turn = st.get("last_turn_id") != before.get("last_turn_id")
            if att and (not att_before or new_turn or att.get("kind") != att_before.get("kind")):
                events.append({"type": "attention", **_watch_summary(st)})
            elif st.get("last_turn_status") not in (None, "inProgress") and (
                before.get("last_turn_status") == "inProgress" or new_turn
            ):
                events.append({"type": "turn_finished", **_watch_summary(st)})
        prev = cur

    return {
        "timedOut": False,
        "events": events,
        "polls": polls,
        "threads": [_watch_summary(st) for st in prev.values()],
    }


def _fleet_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _path_contains(outer: Path, inner: Path) -> bool:
    """True when `inner` is `outer` itself or sits somewhere below it."""
    return outer == inner or outer in inner.parents


def validate_fleet_spec(
    specs: list[dict[str, Any]],
    spec_dir: Path,
    warnings: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate a multi-session fleet launch specification before touching the daemon.

    Hard errors (raise ValueError / FileNotFoundError):
    - Each item must declare `name` and `cwd`; slot numbers must be unique.
    - Two writer slots must not write in the same directory. A writer's write
      root is its `worktree`, or its `cwd` when it has no worktree, so two
      worktree-less writers sharing one `cwd` are rejected.
    - No two slots may claim the same `branch` (git refuses to check one branch
      out in two worktrees, and two writers on one branch clobber each other).
    - No two slots may share an `output_dir`, or nest one inside another.
    - At most one slot may set `cluster_write_owner: true`.
    - `prompt_file` paths (if provided) must exist.

    Soft findings are appended to `warnings` (when a list is passed):
    - A writer's write root nested inside another writer's root (legitimate for
      .gitignored in-repo worktrees, but the outer slot can reach the inner one).
    - A writer slot without its own worktree in a fleet with several writers.
    - `danger-full-access` sandbox or `never` approval policy.
    """
    if not isinstance(specs, list) or not specs:
        raise ValueError("Fleet spec must be a non-empty JSON array of session specs.")

    found_warnings: list[str] = []
    seen_slots: dict[Any, str] = {}
    seen_branches: dict[str, str] = {}
    cluster_owners: list[str] = []
    writer_roots: list[tuple[str, Path, bool]] = []  # (name, write root, has_worktree)
    out_dirs: list[tuple[str, Path]] = []
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
        if sandbox == "danger-full-access":
            found_warnings.append(
                f"{name!r} uses sandbox 'danger-full-access'; only launch it if the user explicitly authorized that."
            )
        if approval_policy == "never":
            found_warnings.append(
                f"{name!r} uses approval_policy 'never'; only launch it if the user explicitly authorized that."
            )

        slot = raw.get("slot", idx + 1)
        if slot in seen_slots:
            raise ValueError(f"Duplicate slot {slot!r}: used by both {seen_slots[slot]!r} and {name!r}.")
        seen_slots[slot] = name

        branch = raw.get("branch")
        if branch:
            if branch in seen_branches:
                raise ValueError(
                    f"Branch isolation collision: {branch!r} assigned to both "
                    f"{seen_branches[branch]!r} and {name!r}."
                )
            seen_branches[branch] = name

        if raw.get("cluster_write_owner"):
            cluster_owners.append(name)

        is_readonly = sandbox == "read-only" or bool(raw.get("read_only"))
        if not is_readonly:
            wt = raw.get("worktree")
            writer_roots.append((name, _fleet_path(wt or cwd), bool(wt)))

        out_dir = raw.get("output_dir")
        if out_dir:
            out_dirs.append((name, _fleet_path(out_dir)))

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
        item["caption_guide"] = bool(raw.get("caption_guide", True))
        item["autonomy_guard"] = bool(raw.get("autonomy_guard", True))
        normalized.append(item)

    if len(cluster_owners) > 1:
        raise ValueError(
            "At most one slot may set cluster_write_owner: true; found "
            + ", ".join(repr(n) for n in cluster_owners) + "."
        )

    for i, (name_a, root_a, wt_a) in enumerate(writer_roots):
        for name_b, root_b, wt_b in writer_roots[i + 1 :]:
            if root_a == root_b:
                kind = "Worktree" if (wt_a and wt_b) else "Writer"
                raise ValueError(
                    f"{kind} isolation collision: {str(root_a)!r} is the write root of both "
                    f"{name_a!r} and {name_b!r}. Give each writer slot its own git worktree, "
                    "or mark one read_only."
                )
            if _path_contains(root_a, root_b) or _path_contains(root_b, root_a):
                outer, inner = (name_a, name_b) if _path_contains(root_a, root_b) else (name_b, name_a)
                found_warnings.append(
                    f"Write root of {inner!r} is nested inside the write root of {outer!r}; "
                    f"make sure {outer!r} never edits the nested directory."
                )

    if len(writer_roots) > 1:
        for name, _root, has_wt in writer_roots:
            if not has_wt:
                found_warnings.append(
                    f"Writer slot {name!r} has no dedicated worktree and writes in its cwd."
                )

    for i, (name_a, out_a) in enumerate(out_dirs):
        for name_b, out_b in out_dirs[i + 1 :]:
            if out_a == out_b:
                raise ValueError(
                    f"Output directory isolation collision: {str(out_a)!r} assigned to both "
                    f"{name_a!r} and {name_b!r}."
                )
            if _path_contains(out_a, out_b) or _path_contains(out_b, out_a):
                raise ValueError(
                    f"Output directory isolation collision: {name_a!r} ({str(out_a)!r}) and "
                    f"{name_b!r} ({str(out_b)!r}) are nested; give each slot a sibling directory."
                )

    if warnings is not None:
        warnings.extend(found_warnings)
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


def _load_registry_entries(registry_path: str) -> list[dict[str, Any]]:
    data = json.loads(Path(registry_path).read_text(encoding="utf-8"))
    sessions = data.get("sessions") if isinstance(data, dict) else data
    if not isinstance(sessions, list):
        raise ValueError(f"Invalid fleet registry format in {registry_path}")
    entries = []
    for item in sessions:
        if isinstance(item, dict):
            tid = item.get("id") or item.get("thread_id") or item.get("threadId")
            if tid:
                normalized = dict(item)
                normalized["id"] = str(tid)
                entries.append(normalized)
    return entries


def _load_ids_from_registry(registry_path: str) -> list[str]:
    return [entry["id"] for entry in _load_registry_entries(registry_path)]


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
    s_list.add_argument(
        "--limit", type=int, default=25, help="Maximum number of threads to return (pages are followed until reached)."
    )
    s_list.add_argument(
        "--archived", action="store_true", default=None, help="List archived threads instead of active ones."
    )
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
    s_status.add_argument(
        "--attention-only",
        action="store_true",
        help="Only print threads whose `attention` is set (waiting on a question, approval, or decision).",
    )

    # watch
    s_watch = sub.add_parser(
        "watch",
        parents=[sub_common],
        help="Block until a watched thread finishes a turn or needs attention (exit 4 on timeout).",
    )
    s_watch.add_argument("ids", nargs="*", help="Thread UUID(s).")
    s_watch.add_argument("--registry", default=None, help="Load thread IDs from a FLEET-REGISTRY.json file.")
    s_watch.add_argument("--interval", type=float, default=15.0, help="Seconds between polls (minimum 1, default 15).")
    s_watch.add_argument("--max-wait", type=float, default=3600.0, help="Give up after this many seconds (default 3600).")
    s_watch.add_argument(
        "--ignore-current",
        action="store_true",
        help="Do not return for attention that already exists at start; report only new events.",
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
    s_create.add_argument(
        "--no-caption-guide",
        action="store_true",
        help="Do not attach the default plain principle-first turn caption instruction.",
    )
    s_create.add_argument(
        "--no-autonomy-guard",
        action="store_true",
        help="Do not attach the default instruction to decide reversible choices and ask only via [DECISION-NEEDED].",
    )
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
        help="Smart non-blocking dispatch (`turn/steer` if active, `thread/resume` + `turn/start` if unloaded (unarchiving first if archived), `turn/start` if idle, interrupt + `turn/start` if blocked on request_user_input).",
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

    s_ret = sub.add_parser(
        "retire",
        parents=[sub_common],
        help="Archive sessions with no follow-up; skips running turns, pending questions, and failed/interrupted last turns.",
    )
    s_ret.add_argument("ids", nargs="*", help="Thread UUID(s).")
    s_ret.add_argument("--registry", default=None, help="Load thread IDs from a FLEET-REGISTRY.json file.")
    s_ret.add_argument("--dry-run", action="store_true", help="Report what would be archived without archiving.")
    s_ret.add_argument(
        "--force",
        action="store_true",
        help="Also archive threads with pending attention or a failed/interrupted last turn (never a running turn).",
    )

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

    # caption
    s_cap = sub.add_parser(
        "caption",
        parents=[sub_common],
        help="Render a readable, principle-first plain caption for one or more sessions (or offline rollout).",
    )
    s_cap.add_argument("ids", nargs="*", help="Thread UUID(s).")
    s_cap.add_argument("--registry", default=None, help="Load session slots from a FLEET-REGISTRY.json file.")
    s_cap.add_argument("--path", default=None, help="Direct local path to rollout JSONL (no daemon needed).")
    s_cap.add_argument("--max-chars", type=int, default=1200, help="Maximum caption length in characters (default: 1200).")
    s_cap.add_argument(
        "--show-policy",
        action="store_true",
        help="Include a compact one-line runtime policy summary (omitted by default to avoid noise).",
    )
    s_cap.add_argument("--json", action="store_true", help="Emit structured JSON instead of plain text.")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Every command, including the offline and dry-run paths, reports failures as a
    # JSON error object (exit 1, or 3 for quota exhaustion) instead of a traceback.
    try:
        # Offline path for inspect-rollout --path (no daemon required).
        if args.cmd == "inspect-rollout" and args.path and not args.thread:
            _print_json(inspect_rollout_file(args.path, last_n=args.last_n), pretty=args.pretty)
            return 0

        # Offline path for caption --path (no daemon required).
        if args.cmd == "caption" and args.path and not args.ids and not args.registry:
            rec = rollout_caption(args.path, max_chars=args.max_chars)
            if args.json:
                _print_json(rec, pretty=args.pretty)
            else:
                print(format_session_caption(rec, show_policy=args.show_policy), flush=True)
            return 0

        if args.cmd == "launch-batch" and args.dry_run:
            spec_path = Path(args.spec).resolve()
            raw_specs = json.loads(spec_path.read_text(encoding="utf-8"))
            warnings: list[str] = []
            validated = validate_fleet_spec(raw_specs, spec_path.parent, warnings)
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
                    "caption_guide": item["caption_guide"],
                    "autonomy_guard": item["autonomy_guard"],
                    "has_prompt": item["resolved_prompt"] is not None,
                }
                for i, item in enumerate(validated)
            ]
            _print_json(
                {"dry_run": True, "count": len(summary), "warnings": warnings, "sessions": summary},
                pretty=args.pretty,
            )
            return 0

        if args.cmd == "watch":
            ids = list(args.ids)
            if args.registry:
                ids.extend(_load_ids_from_registry(args.registry))
            if not ids:
                parser.error("watch requires at least one thread ID or --registry.")
            result = watch_threads(
                lambda: CodexAppServerClient(sock_path=args.socket, timeout=args.timeout),
                list(dict.fromkeys(ids)),
                interval=max(1.0, args.interval),
                max_wait=max(0.0, args.max_wait),
                ignore_current=args.ignore_current,
            )
            _print_json(result, pretty=args.pretty)
            return EXIT_WATCH_TIMEOUT if result["timedOut"] else 0

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
                    if args.attention_only and not res.get("attention"):
                        continue
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
                    caption_guide=not args.no_caption_guide,
                    autonomy_guard=not args.no_autonomy_guard,
                    ephemeral=args.ephemeral,
                )
                _print_json(res, pretty=args.pretty)

            elif args.cmd == "launch-batch":
                spec_path = Path(args.spec).resolve()
                raw_specs = json.loads(spec_path.read_text(encoding="utf-8"))
                batch_warnings: list[str] = []
                validated = validate_fleet_spec(raw_specs, spec_path.parent, batch_warnings)
                if batch_warnings:
                    print(json.dumps({"warnings": batch_warnings}, ensure_ascii=False), file=sys.stderr, flush=True)
                if args.check_quota:
                    cli.check_quota_or_raise()
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
                            caption_guide=bool(item.get("caption_guide", True)),
                            autonomy_guard=bool(item.get("autonomy_guard", True)),
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
                    "validation_warnings": batch_warnings,
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

            elif args.cmd == "retire":
                ids = list(args.ids)
                if args.registry:
                    ids.extend(_load_ids_from_registry(args.registry))
                if not ids:
                    parser.error("retire requires at least one thread ID or --registry.")
                # One thread's failure must not abort the batch: report it and go on.
                failed = 0
                for tid in dict.fromkeys(ids):
                    try:
                        rec = cli.retire_thread(tid, dry_run=args.dry_run, force=args.force)
                    except (EOFError, ConnectionError, socket.timeout):
                        raise  # the daemon connection itself failed; later threads would too
                    except (CodexRPCError, RolloutReadError, OSError) as exc:
                        failed += 1
                        rec = {"id": tid, "action": "error", "error": type(exc).__name__, "detail": str(exc)}
                        if isinstance(exc, CodexRPCError):
                            rec["method"] = exc.method
                    _print_json(rec, pretty=args.pretty)
                if failed:
                    return 1

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

            elif args.cmd == "caption":
                targets: list[tuple[str, dict[str, Any] | None]] = [(tid, None) for tid in args.ids]
                if args.registry:
                    for entry in _load_registry_entries(args.registry):
                        targets.append((entry["id"], entry))
                if not targets:
                    parser.error("caption requires at least one thread ID, --registry, or --path.")
                records = [
                    cli.thread_caption(tid, max_chars=args.max_chars, registry_meta=meta)
                    for tid, meta in targets
                ]
                if args.json:
                    payload: Any = records[0] if len(records) == 1 else {"count": len(records), "captions": records}
                    _print_json(payload, pretty=args.pretty)
                else:
                    blocks = [format_session_caption(r, show_policy=args.show_policy) for r in records]
                    print("\n\n".join(blocks), flush=True)

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
