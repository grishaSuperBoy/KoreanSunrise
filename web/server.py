"""
web/server.py - Built-in HTTP Server and API for Korean Sunrise Intelligence Hub.
Provides live monitoring, reporting hub, threshold calibration, 1-click dataset export,
and hardened security (zero-token URL leaks, SameSite HttpOnly cookies, and anti-CSRF protection).
"""
from __future__ import annotations

import datetime
import hashlib
import hmac
import http.server
import json
import logging
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from web.reports_manager import list_available_reports, get_report_content
from web.views import render_dashboard_html, render_login_html

log = logging.getLogger("api_server")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)


class APIServer:
    """Lightweight built-in HTTP server for Korean Sunrise monitoring and controls."""

    def __init__(
        self,
        service: Any,
        host: str = "0.0.0.0",
        port: int = 8089,
        token: str = "vitali_secret_key_2026",
    ):
        self.service = service
        self.host = host
        self.port = port
        self.token = token
        self._csrf_secret: str = secrets.token_hex(16)
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # In-memory mtime cache for matrix thresholds to avoid synchronous disk I/O on active dashboard
        self._cached_thresholds: Dict[str, Any] = {}
        self._thresholds_mtime: float = 0.0
        self._thresholds_lock = threading.Lock()
        self._pipeline_proc: Optional[subprocess.Popen] = None
        self._pipeline_lock = threading.Lock()
        self._report_lock = threading.Lock()
        self._calib_lock = threading.Lock()

    def get_thresholds_doc(self) -> Dict[str, Any]:
        """Returns cached matrix thresholds document, reloading from disk only when mtime changes."""
        th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
        try:
            if not os.path.isfile(th_file):
                return {}
            mtime = os.path.getmtime(th_file)
            with self._thresholds_lock:
                if mtime != self._thresholds_mtime or not self._cached_thresholds:
                    with open(th_file, "r", encoding="utf-8") as tf:
                        self._cached_thresholds = json.load(tf)
                    self._thresholds_mtime = mtime
                return self._cached_thresholds
        except Exception as e:
            log.warning(f"[api_server] Error reading thresholds: {e}")
            with self._thresholds_lock:
                return self._cached_thresholds or {}

    def get_csrf_token(self, token: Optional[str] = None) -> str:
        """Derives a cryptographically secure anti-CSRF token bound to the server session and token."""
        tok = token or self.token
        if not tok:
            return ""
        key = f"{tok}:{self._csrf_secret}".encode("utf-8")
        return hmac.new(key, b"korean_sunrise_csrf_v1", hashlib.sha256).hexdigest()[:32]

    def verify_csrf_token(self, candidate: str) -> bool:
        """Validates candidate anti-CSRF token using timing-safe comparison."""
        if not candidate or not self.token:
            return False
        expected = self.get_csrf_token()
        return hmac.compare_digest(candidate, expected)

    def start(self) -> None:
        if self._running:
            return

        server_ref = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                log.debug(f"[api] {self.address_string()} - {format % args}")

            def _get_auth_info(self) -> Tuple[str, str]:
                """
                Extracts authentication credentials and identifies the source ('header' or 'cookie').
                Query parameter (?token=...) authentication is strictly rejected to prevent secret
                leakage into web server access logs, reverse proxy logs, and browser history.
                """
                auth_hdr = self.headers.get("Authorization", "")
                if auth_hdr.startswith("Bearer "):
                    return auth_hdr.split(" ", 1)[1].strip(), "header"
                if "X-Token" in self.headers:
                    return self.headers["X-Token"].strip(), "header"
                cookies = self.headers.get("Cookie", "")
                for part in cookies.split(";"):
                    if "=" in part:
                        k, v = part.strip().split("=", 1)
                        if k == "token":
                            return v.strip(), "cookie"
                return "", ""

            def _is_authenticated(self) -> bool:
                if not server_ref.token:
                    return True
                tok, _ = self._get_auth_info()
                return bool(tok) and hmac.compare_digest(tok, server_ref.token)

            def _validate_csrf(self, body_bytes: bytes) -> bool:
                """
                Validates anti-CSRF tokens for state-changing requests originating from cookie sessions.
                Checks X-CSRF-Token / X-CSRFToken header first, then inspects body payload.
                """
                header_csrf = self.headers.get("X-CSRF-Token") or self.headers.get("X-CSRFToken")
                if header_csrf and server_ref.verify_csrf_token(header_csrf.strip()):
                    return True

                if body_bytes:
                    content_type = self.headers.get("Content-Type", "")
                    if "application/x-www-form-urlencoded" in content_type:
                        try:
                            form = urllib.parse.parse_qs(body_bytes.decode("utf-8", errors="replace"))
                            cand = form.get("csrf_token", [""])[0]
                            if cand and server_ref.verify_csrf_token(cand):
                                return True
                        except Exception:
                            pass
                    elif "application/json" in content_type:
                        try:
                            payload = json.loads(body_bytes.decode("utf-8", errors="replace"))
                            if isinstance(payload, dict):
                                cand = payload.get("csrf_token", "")
                                if cand and server_ref.verify_csrf_token(str(cand)):
                                    return True
                        except Exception:
                            pass

                return False

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                q = urllib.parse.parse_qs(parsed.query)
                is_auth = self._is_authenticated()

                if path in ("/", "/dashboard", "/status"):
                    if not is_auth:
                        self._send_login()
                    else:
                        self._handle_dashboard()
                elif not is_auth:
                    self._send_unauthorized()
                elif path == "/stats":
                    self._handle_stats()
                elif path == "/pool":
                    self._handle_pool()
                elif path == "/battle":
                    self._handle_battle()
                elif path in ("/api/trading/profile", "/profile"):
                    self._handle_profile()
                elif path in ("/api/thresholds", "/thresholds"):
                    self._handle_thresholds()
                elif path in ("/api/report", "/report"):
                    self._handle_reports_list()
                elif path == "/api/report/content":
                    self._handle_report_content(q)
                elif path == "/download":
                    self._handle_download(q)
                else:
                    self._send_404()

            def do_POST(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"

                length = int(self.headers.get("Content-Length", 0))
                body_bytes = self.rfile.read(length) if length > 0 else b""

                # 1. Public Authentication Route
                if path == "/login":
                    self._handle_login(body_bytes)
                    return

                # 2. General Authentication Check
                tok, auth_source = self._get_auth_info()
                if not tok or not hmac.compare_digest(tok, server_ref.token):
                    self._send_unauthorized()
                    return

                # 3. Anti-CSRF Verification for Browser/Cookie Sessions
                # Requests authenticated via custom headers (Bearer/X-Token) are exempt as
                # browsers cannot forge custom cross-origin headers without CORS preflight.
                if auth_source == "cookie":
                    if not self._validate_csrf(body_bytes):
                        self._send_forbidden("CSRF validation failed: missing or invalid anti-CSRF token")
                        return

                # 4. State-Changing Protected Routes
                if path == "/logout":
                    self._handle_logout()
                elif path == "/api/report/generate":
                    self._handle_generate_report()
                elif path == "/api/thresholds/calibrate":
                    self._handle_calibrate_thresholds()
                elif path == "/api/daily-pipeline":
                    self._handle_trigger_pipeline()
                elif path in ("/pool/pause", "/pool/resume"):
                    self._handle_pool_toggle(path, body_bytes)
                else:
                    self._send_404()

            # Handlers
            def _send_login(self, error: Optional[str] = None) -> None:
                html_body = render_login_html(error=error)
                self._send_html(html_body, code=200 if not error else 401)

            def _handle_login(self, body_bytes: bytes) -> None:
                content_type = self.headers.get("Content-Type", "")
                submitted_token = ""
                is_form = False

                if "application/json" in content_type:
                    try:
                        payload = json.loads(body_bytes.decode("utf-8", errors="replace"))
                        submitted_token = payload.get("token", "")
                    except Exception:
                        pass
                elif "application/x-www-form-urlencoded" in content_type:
                    is_form = True
                    try:
                        form = urllib.parse.parse_qs(body_bytes.decode("utf-8", errors="replace"))
                        submitted_token = form.get("token", [""])[0]
                    except Exception:
                        pass
                else:
                    body_text = body_bytes.decode("utf-8", errors="replace").strip()
                    if body_text.startswith("token="):
                        is_form = True
                        form = urllib.parse.parse_qs(body_text)
                        submitted_token = form.get("token", [""])[0]
                    else:
                        submitted_token = body_text

                if not server_ref.token or hmac.compare_digest(submitted_token, server_ref.token):
                    csrf_tok = server_ref.get_csrf_token(server_ref.token)
                    cookie_tok = f"token={server_ref.token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400"
                    cookie_csrf = f"csrf_token={csrf_tok}; Path=/; SameSite=Strict; Max-Age=86400"

                    if is_form or "text/html" in self.headers.get("Accept", ""):
                        self.send_response(303)
                        self.send_header("Location", "/dashboard")
                        self.send_header("Set-Cookie", cookie_tok)
                        self.send_header("Set-Cookie", cookie_csrf)
                        self.end_headers()
                    else:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Set-Cookie", cookie_tok)
                        self.send_header("Set-Cookie", cookie_csrf)
                        b = json.dumps({"ok": True, "csrf_token": csrf_tok}).encode("utf-8")
                        self.send_header("Content-Length", str(len(b)))
                        self.end_headers()
                        self.wfile.write(b)
                else:
                    time.sleep(0.5)
                    if is_form or "text/html" in self.headers.get("Accept", ""):
                        self._send_login(error="Неверный ключ авторизации")
                    else:
                        self._send_json({"ok": False, "error": "Invalid authentication token"}, code=401)

            def _handle_logout(self) -> None:
                cookie_clear_tok = "token=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
                cookie_clear_csrf = "csrf_token=; Path=/; Max-Age=0; SameSite=Strict"

                if "text/html" in self.headers.get("Accept", "") or "application/x-www-form-urlencoded" in self.headers.get("Content-Type", ""):
                    self.send_response(303)
                    self.send_header("Location", "/")
                    self.send_header("Set-Cookie", cookie_clear_tok)
                    self.send_header("Set-Cookie", cookie_clear_csrf)
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Set-Cookie", cookie_clear_tok)
                    self.send_header("Set-Cookie", cookie_clear_csrf)
                    b = json.dumps({"ok": True, "message": "Logged out successfully"}).encode("utf-8")
                    self.send_header("Content-Length", str(len(b)))
                    self.end_headers()
                    self.wfile.write(b)

            def _handle_dashboard(self) -> None:
                tok, _ = self._get_auth_info()
                effective_tok = tok or server_ref.token
                csrf_tok = server_ref.get_csrf_token(effective_tok)

                cookie_tok_hdr = f"token={effective_tok}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400"
                cookie_csrf_hdr = f"csrf_token={csrf_tok}; Path=/; SameSite=Strict; Max-Age=86400"

                # Collect live status
                status = {}
                pool_snap = {}
                profile = {}
                thresholds = {}

                if hasattr(server_ref.service, "stream_manager") and server_ref.service.stream_manager:
                    status["streams"] = server_ref.service.stream_manager.get_status()
                    status["usdt_krw_rate"] = server_ref.service.stream_manager.get_usdt_krw_rate()
                if hasattr(server_ref.service, "engine") and server_ref.service.engine:
                    status["counters"] = server_ref.service.engine.counters
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    pool_snap = server_ref.service.pool_state.get_pool_snapshot()
                if hasattr(server_ref.service, "profile_manager") and server_ref.service.profile_manager:
                    profile = server_ref.service.profile_manager.get_profile()

                thresholds = server_ref.get_thresholds_doc()
                cards = list_available_reports()
                html_body = render_dashboard_html(
                    service_status=status,
                    pool_snapshot=pool_snap,
                    profile_data=profile,
                    thresholds_doc=thresholds,
                    reports_cards=cards,
                    csrf_token=csrf_tok,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Set-Cookie", cookie_tok_hdr)
                self.send_header("Set-Cookie", cookie_csrf_hdr)
                self.end_headers()
                self.wfile.write(html_body.encode("utf-8"))

            def _handle_stats(self) -> None:
                data = {
                    "running": getattr(server_ref.service, "running", False),
                    "counters": getattr(server_ref.service.engine, "counters", {}) if hasattr(server_ref.service, "engine") else {},
                }
                self._send_json(data)

            def _handle_pool(self) -> None:
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    self._send_json(server_ref.service.pool_state.get_pool_snapshot())
                else:
                    self._send_json({})

            def _handle_battle(self) -> None:
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    self._send_json(server_ref.service.pool_state.get_battle_coins())
                else:
                    self._send_json([])

            def _handle_profile(self) -> None:
                if hasattr(server_ref.service, "profile_manager") and server_ref.service.profile_manager:
                    self._send_json(server_ref.service.profile_manager.get_profile())
                else:
                    self._send_json({})

            def _handle_thresholds(self) -> None:
                self._send_json(server_ref.get_thresholds_doc())

            def _handle_reports_list(self) -> None:
                self._send_json(list_available_reports())

            def _handle_report_content(self, q: Dict[str, List[str]]) -> None:
                rep_id = q.get("id", [""])[0]
                content = get_report_content(rep_id)
                if content:
                    self._send_text(content)
                else:
                    self._send_text("Отчёт не найден.", code=404)

            def _handle_generate_report(self) -> None:
                if not server_ref._report_lock.acquire(blocking=False):
                    self._send_json({"ok": False, "error": "Report generation is already running"}, code=409)
                    return
                try:
                    cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "full_daily_report.py"), "--json"]
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)
                finally:
                    server_ref._report_lock.release()

            def _handle_calibrate_thresholds(self) -> None:
                if not server_ref._calib_lock.acquire(blocking=False):
                    self._send_json({"ok": False, "error": "Thresholds calibration is already running"}, code=409)
                    return
                try:
                    cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "calibrate_thresholds.py"), "--json"]
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    if hasattr(server_ref.service, "engine") and server_ref.service.engine:
                        server_ref.service.engine.reload_thresholds()
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)
                finally:
                    server_ref._calib_lock.release()

            def _handle_trigger_pipeline(self) -> None:
                with server_ref._pipeline_lock:
                    if server_ref._pipeline_proc is not None and server_ref._pipeline_proc.poll() is None:
                        self._send_json({"ok": False, "error": "Daily pipeline is already running in background"}, code=409)
                        return
                    cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "run_daily_pipeline.py")]
                    server_ref._pipeline_proc = subprocess.Popen(cmd)
                self._send_json({"ok": True, "message": "Master 24h pipeline started in background"})

            def _handle_pool_toggle(self, path: str, body_bytes: bytes) -> None:
                try:
                    payload = json.loads(body_bytes.decode("utf-8", errors="replace")) if body_bytes else {}
                    coin = payload.get("coin", "").upper()
                    if not coin:
                        self._send_json({"error": "Missing coin parameter"}, code=400)
                        return
                    if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                        if path == "/pool/pause":
                            server_ref.service.pool_state.pause_coin(coin)
                        else:
                            server_ref.service.pool_state.resume_coin(coin)
                        self._send_json({"ok": True, "coin": coin})
                        return
                except Exception as e:
                    self._send_json({"error": str(e)}, code=400)
                    return
                self._send_json({"error": "pool_state unavailable"}, code=500)

            def _handle_download(self, q: Dict[str, List[str]]) -> None:
                target_file = os.path.basename(q.get("file", [""])[0])
                if target_file:
                    fp = os.path.join(PROJECT_ROOT, "reports", "daily", target_file)
                    if os.path.isfile(fp):
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Disposition", f"attachment; filename=\"{target_file}\"")
                        self.end_headers()
                        with open(fp, "rb") as f:
                            shutil.copyfileobj(f, self.wfile)
                        return
                    self._send_404()
                    return

                # Download zip of configs, reports, and signal archives
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                tmp.close()
                try:
                    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as z:
                        for fn in ["universe_live.json", "live_trading_pool.json", "matrix_thresholds.json", "trading_profile.json", "runtime_overrides.json"]:
                            fp = os.path.join(PROJECT_ROOT, fn)
                            if os.path.isfile(fp):
                                z.write(fp, arcname=fn)
                        rep_dir = os.path.join(PROJECT_ROOT, "reports", "daily")
                        if os.path.isdir(rep_dir):
                            for rfn in os.listdir(rep_dir):
                                rfp = os.path.join(rep_dir, rfn)
                                if os.path.isfile(rfp):
                                    z.write(rfp, arcname=os.path.join("reports", "daily", rfn))
                        sig_dir = os.path.join(PROJECT_ROOT, "data", "signals")
                        if os.path.isdir(sig_dir):
                            for root, _, files in os.walk(sig_dir):
                                for sfn in files:
                                    if sfn.endswith(".jsonl.gz"):
                                        full_p = os.path.join(root, sfn)
                                        rel_p = os.path.relpath(full_p, PROJECT_ROOT)
                                        z.write(full_p, arcname=rel_p)

                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", "attachment; filename=\"korean_sunrise_export.zip\"")
                    self.end_headers()
                    with open(tmp.name, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                finally:
                    try:
                        os.remove(tmp.name)
                    except OSError:
                        pass

            # Helpers
            def _send_json(self, data: Any, code: int = 200) -> None:
                b = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_html(self, html_str: str, code: int = 200) -> None:
                b = html_str.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_text(self, text: str, code: int = 200) -> None:
                b = text.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_unauthorized(self) -> None:
                self._send_json({"error": "Unauthorized. Provide Authorization header or session cookie"}, code=401)

            def _send_forbidden(self, msg: str = "Forbidden") -> None:
                self._send_json({"error": msg}, code=403)

            def _send_404(self) -> None:
                self._send_json({"error": "Not Found"}, code=404)

        try:
            self._server = http.server.ThreadingHTTPServer((self.host, self.port), Handler)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            self._running = True
            log.info(f"=== Korean Sunrise Web API Server running at http://{self.host}:{self.port}/ (Auth token protected) ===")
        except Exception as e:
            log.error(f"[api_server] Failed to start HTTP server on port {self.port}: {e}")

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        log.info("[api_server] Web API Server stopped cleanly.")
