"""
tests/test_web_security.py - Security tests for APIServer in Korean Sunrise.
Validates zero-token query leaks, cookie sessions, and anti-CSRF protection.
"""
from __future__ import annotations

import json
import unittest
import urllib.request
import urllib.error
import http.client
from unittest.mock import MagicMock

from web.server import APIServer


class TestWebSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mock_service = MagicMock()
        cls.mock_service.running = True
        cls.mock_service.engine.counters = {"signals_triggered": 42}
        cls.mock_service.pool_state.get_pool_snapshot.return_value = {"summary": {"battle": 5}}
        cls.mock_service.pool_state.pause_coin = MagicMock()
        cls.mock_service.pool_state.resume_coin = MagicMock()
        cls.mock_service.stream_manager.get_status.return_value = {}
        cls.mock_service.stream_manager.get_usdt_krw_rate.return_value = 1380.0

        cls.token = "super_secret_test_token_2026"
        cls.server = APIServer(service=cls.mock_service, host="127.0.0.1", port=0, token=cls.token)
        cls.server.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def test_query_param_token_is_rejected_for_auth(self) -> None:
        """Verify that ?token=... does not authenticate protected API endpoints."""
        req = urllib.request.Request(f"{self.base_url}/stats?token={self.token}")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        self.assertEqual(ctx.exception.code, 401)

    def test_unauthenticated_dashboard_serves_login(self) -> None:
        """Visiting / without credentials returns 200 with the login form."""
        req = urllib.request.Request(f"{self.base_url}/")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            html = resp.read().decode("utf-8")
            self.assertIn("action=\"/login\"", html)
            self.assertIn("Секретный ключ авторизации", html)

    def test_login_post_json_success_and_cookies(self) -> None:
        """POST /login with valid JSON token returns csrf_token and sets secure cookies."""
        req = urllib.request.Request(
            f"{self.base_url}/login",
            data=json.dumps({"token": self.token}).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertTrue(data.get("ok"))
            csrf_token = data.get("csrf_token")
            self.assertTrue(bool(csrf_token))

            # Inspect Set-Cookie headers
            set_cookies = resp.headers.get_all("Set-Cookie")
            cookie_text = " ".join(set_cookies)
            self.assertIn(f"token={self.token}", cookie_text)
            self.assertIn("HttpOnly", cookie_text)
            self.assertIn("SameSite=Strict", cookie_text)
            self.assertIn(f"csrf_token={csrf_token}", cookie_text)

    def test_login_post_invalid_token(self) -> None:
        """POST /login with invalid token returns 401."""
        req = urllib.request.Request(
            f"{self.base_url}/login",
            data=json.dumps({"token": "wrong_token"}).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        self.assertEqual(ctx.exception.code, 401)

    def test_csrf_protection_blocks_post_from_cookie_session_without_csrf_token(self) -> None:
        """A POST from a browser cookie session without X-CSRF-Token MUST return 403 Forbidden."""
        req = urllib.request.Request(
            f"{self.base_url}/pool/pause",
            data=json.dumps({"coin": "DOGE"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Cookie": f"token={self.token}"  # Simulating authenticated browser session
            }
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        self.assertEqual(ctx.exception.code, 403)
        body = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertIn("CSRF validation failed", body.get("error", ""))

    def test_csrf_protection_blocks_post_with_invalid_csrf_token(self) -> None:
        """A POST with a fake/wrong CSRF token MUST return 403 Forbidden."""
        req = urllib.request.Request(
            f"{self.base_url}/pool/pause",
            data=json.dumps({"coin": "DOGE"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Cookie": f"token={self.token}",
                "X-CSRF-Token": "bogus_attacker_csrf_token"
            }
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        self.assertEqual(ctx.exception.code, 403)

    def test_csrf_protection_allows_post_with_valid_csrf_token(self) -> None:
        """A POST with valid cookie and matching X-CSRF-Token succeeds."""
        valid_csrf = self.server.get_csrf_token()
        req = urllib.request.Request(
            f"{self.base_url}/pool/pause",
            data=json.dumps({"coin": "SOL"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Cookie": f"token={self.token}",
                "X-CSRF-Token": valid_csrf
            }
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertTrue(data.get("ok"))
            self.assertEqual(data.get("coin"), "SOL")
            self.mock_service.pool_state.pause_coin.assert_called_with("SOL")

    def test_bearer_token_api_exempt_from_csrf(self) -> None:
        """API requests authenticated via Authorization: Bearer token do not require CSRF token."""
        req = urllib.request.Request(
            f"{self.base_url}/pool/resume",
            data=json.dumps({"coin": "XRP"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}"
            }
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertTrue(data.get("ok"))
            self.assertEqual(data.get("coin"), "XRP")
            self.mock_service.pool_state.resume_coin.assert_called_with("XRP")

    def test_x_token_api_exempt_from_csrf(self) -> None:
        """API requests authenticated via X-Token header do not require CSRF token."""
        req = urllib.request.Request(
            f"{self.base_url}/pool/pause",
            data=json.dumps({"coin": "ETH"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Token": self.token
            }
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertTrue(data.get("ok"))

    def test_logout_clears_cookies(self) -> None:
        """POST /logout clears token and csrf_token cookies."""
        req = urllib.request.Request(
            f"{self.base_url}/logout",
            data=b"",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            set_cookies = resp.headers.get_all("Set-Cookie")
            cookie_text = " ".join(set_cookies)
            self.assertIn("token=;", cookie_text)
            self.assertIn("csrf_token=;", cookie_text)
            self.assertIn("Max-Age=0", cookie_text)

    def test_thresholds_cached_in_memory(self) -> None:
        """Verify get_thresholds_doc caches in memory and avoids disk re-reads."""
        doc1 = self.server.get_thresholds_doc()
        self.assertIsInstance(doc1, dict)
        doc2 = self.server.get_thresholds_doc()
        self.assertIs(doc1, doc2, "Cached thresholds doc should return the identical in-memory dict instance")

    def test_login_invalid_token_delayed(self) -> None:
        """Verify invalid login triggers anti-bruteforce delay (>= 0.4s)."""
        import time
        t0 = time.time()
        req = urllib.request.Request(
            f"{self.base_url}/login",
            data=json.dumps({"token": "wrong_password_attempt"}).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        elapsed = time.time() - t0
        self.assertEqual(ctx.exception.code, 401)
        self.assertGreaterEqual(elapsed, 0.45)

    def test_pipeline_concurrent_trigger_blocked(self) -> None:
        """Verify triggering pipeline while running returns 409 Conflict."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # Process is still running
        self.server._pipeline_proc = mock_proc
        try:
            req = urllib.request.Request(
                f"{self.base_url}/api/daily-pipeline",
                data=b"",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                }
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req)
            self.assertEqual(ctx.exception.code, 409)
        finally:
            self.server._pipeline_proc = None

    def test_token_with_special_characters_cookie(self) -> None:
        """Tokens containing special characters (;, =, spaces) work with quoted cookies."""
        import urllib.parse
        special_token = "tok;with,special=chars and spaces!"
        server = APIServer(service=self.mock_service, host="127.0.0.1", port=0, token=special_token)
        server.start()
        try:
            url = f"http://127.0.0.1:{server.port}"
            # 1. Login with special token
            req = urllib.request.Request(
                f"{url}/login",
                data=json.dumps({"token": special_token}).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                cookie_hdrs = resp.headers.get_all("Set-Cookie")
                cookie_str = "; ".join(cookie_hdrs)
                # Verify token is URL-quoted in Set-Cookie
                quoted_tok = urllib.parse.quote(special_token, safe="")
                self.assertIn(f"token={quoted_tok}", cookie_str)

            # 2. Access dashboard stats using the quoted cookie
            req_dash = urllib.request.Request(
                f"{url}/stats",
                headers={"Cookie": f"token={quoted_tok}"}
            )
            with urllib.request.urlopen(req_dash) as resp_dash:
                self.assertEqual(resp_dash.status, 200)
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main()
