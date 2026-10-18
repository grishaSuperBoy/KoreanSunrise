"""
web/server.py - Built-in HTTP Server and API for Korean Sunrise Intelligence Hub.
Provides live monitoring, reporting hub, threshold calibration, and 1-click dataset export.
"""
from __future__ import annotations

import datetime
import http.server
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from typing import Any, Dict, List, Optional

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
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return

        server_ref = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                log.debug(f"[api] {self.address_string()} - {format % args}")

            def _get_token(self, q: Dict[str, List[str]]) -> str:
                if "token" in q and q["token"]:
                    return q["token"][0]
                auth_hdr = self.headers.get("Authorization", "")
                if auth_hdr.startswith("Bearer "):
                    return auth_hdr.split(" ", 1)[1].strip()
                if "X-Token" in self.headers:
                    return self.headers["X-Token"].strip()
                cookies = self.headers.get("Cookie", "")
                for part in cookies.split(";"):
                    if "=" in part:
                        k, v = part.strip().split("=", 1)
                        if k == "token":
                            return v
                return ""

            def _is_authenticated(self, q: Dict[str, List[str]]) -> bool:
                if not server_ref.token:
                    return True
                return self._get_token(q) == server_ref.token

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                q = urllib.parse.parse_qs(parsed.query)
                is_auth = self._is_authenticated(q)

                if path in ("/", "/dashboard", "/status"):
                    if not is_auth:
                        self._send_login()
                    else:
                        self._handle_dashboard(q)
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
                q = urllib.parse.parse_qs(parsed.query)

                if not self._is_authenticated(q):
                    self._send_unauthorized()
                    return

                if path == "/api/report/generate":
                    self._handle_generate_report()
                elif path == "/api/thresholds/calibrate":
                    self._handle_calibrate_thresholds()
                elif path == "/api/daily-pipeline":
                    self._handle_trigger_pipeline()
                elif path in ("/pool/pause", "/pool/resume"):
                    self._handle_pool_toggle(path)
                else:
                    self._send_404()

            # Handlers
            def _send_login(self) -> None:
                html_body = render_login_html()
                self._send_html(html_body, code=200)

            def _handle_dashboard(self, q: Dict[str, List[str]]) -> None:
                tok = self._get_token(q) or server_ref.token
                cookie_hdr = f"token={tok}; Path=/; HttpOnly; SameSite=Strict"

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

                th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
                if os.path.exists(th_file):
                    try:
                        with open(th_file, "r", encoding="utf-8") as tf:
                            thresholds = json.load(tf)
                    except Exception:
                        pass

                cards = list_available_reports()
                html_body = render_dashboard_html(
                    service_status=status,
                    pool_snapshot=pool_snap,
                    profile_data=profile,
                    thresholds_doc=thresholds,
                    reports_cards=cards,
                    auth_token=tok,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Set-Cookie", cookie_hdr)
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
                th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
                if os.path.exists(th_file):
                    try:
                        with open(th_file, "r", encoding="utf-8") as tf:
                            self._send_json(json.load(tf))
                            return
                    except Exception as e:
                        self._send_json({"error": str(e)}, code=500)
                        return
                self._send_json({"thresholds": {}})

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
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "full_daily_report.py"), "--json"]
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)

            def _handle_calibrate_thresholds(self) -> None:
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "calibrate_thresholds.py"), "--json"]
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    if hasattr(server_ref.service, "engine") and server_ref.service.engine:
                        server_ref.service.engine.reload_thresholds()
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)

            def _handle_trigger_pipeline(self) -> None:
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "run_daily_pipeline.py")]
                subprocess.Popen(cmd)
                self._send_json({"ok": True, "message": "Master 24h pipeline started in background"})

            def _handle_pool_toggle(self, path: str) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode("utf-8")
                try:
                    payload = json.loads(body)
                    coin = payload.get("coin", "").upper()
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
                self._send_json({"error": "Unauthorized. Provide valid ?token= or Bearer token"}, code=401)

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
