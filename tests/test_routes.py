"""
tests/test_routes.py

Integration tests for web/backend.py routes using Flask's test client.

All DB calls are patched. No real MySQL, no network.

Sections:
  - Public routes  (/, /search, /api/stats, /api/suggestions)
  - Auth           (login, logout, admin_required guard)
  - CSRF           (mutating endpoints reject missing/bad tokens)
  - Admin API      (/api/job/*, /api/missing_titles, /api/delete_title_results)
  - MAL endpoints  (/api/mal/mangalist, /api/mal/set_filter, /api/mal/clear_filter)
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

from tests.conftest import post_json


# ── Public routes ─────────────────────────────────────────────────────────────


class TestPublicRoutes:
    def test_home_returns_200(self, client):
        with (
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = client.get("/")
        assert resp.status_code == 200

    def test_home_calls_render_template(self, client):
        with (
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>") as mock_rt,
        ):
            client.get("/")
        mock_rt.assert_called_once()
        args, kwargs = mock_rt.call_args
        assert args[0] == "index.html"
        assert kwargs.get("LCPL_LIBRARY_ID") == 1
        assert kwargs.get("BROWARD_LIBRARY_ID") == 2

    def test_api_stats_returns_json(self, client):
        fake_row = {"volumes": 500, "titles": 42}
        with (
            patch("web.backend.execute_query", return_value=fake_row),
            patch("web.backend.read_job_history", return_value=[]),
        ):
            resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["volumes"] == 500
        assert data["titles"] == 42

    def test_api_stats_last_scraped_from_history(self, client):
        fake_row = {"volumes": 0, "titles": 0}
        history = [
            {
                "job": "scrape_leon",
                "status": "done",
                "at": "2024-06-01T12:00:00+00:00",
                "message": "",
            }
        ]
        with (
            patch("web.backend.execute_query", return_value=fake_row),
            patch("web.backend.read_job_history", return_value=history),
        ):
            resp = client.get("/api/stats")
        data = resp.get_json()
        assert "Scraped" in data["last_scraped"]

    def test_api_suggestions_too_short_returns_empty(self, client):
        resp = client.get("/api/suggestions?q=a")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_api_suggestions_returns_matches(self, client):
        fake_rows = [{"MangaID": 1, "Title": "Berserk", "Type": "Manga", "Score": 9.4}]
        with patch("web.backend.execute_query", return_value=fake_rows):
            resp = client.get("/api/suggestions?q=Berserk")
        assert resp.status_code == 200
        results = resp.get_json()
        assert len(results) == 1
        assert results[0]["title"] == "Berserk"

    def test_api_suggestions_db_error_returns_empty(self, client):
        with patch("web.backend.execute_query", side_effect=Exception("db error")):
            resp = client.get("/api/suggestions?q=Berserk")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_search_returns_200_with_mocked_db(self, client):
        with (
            patch("web.backend.execute_query", return_value=[]),
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = client.get("/search?title=Berserk")
        assert resp.status_code == 200

    def test_search_passes_mal_session_data(self, client):
        with client.session_transaction() as sess:
            sess["mal_data"] = {"1": {"status": "reading", "score": 0, "num_volumes_read": 0}}
            sess["mal_filters"] = {"reading": "include", "completed": ""}

        with (
            patch("web.backend.execute_query", return_value=[]),
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>") as mock_rt,
        ):
            client.get("/search")

        _, kwargs = mock_rt.call_args
        assert kwargs.get("mal_loaded") is True
        assert kwargs.get("mal_active") is True


# ── Admin auth ────────────────────────────────────────────────────────────────


class TestAdminAuth:
    def test_admin_redirect_when_not_logged_in(self, client):
        # Patch ADMIN_PASSWORD to a non-empty value so the "no password +
        # localhost" bypass doesn't silently log the test client in.
        with (
            patch("web.backend.ADMIN_PASSWORD", "required"),
            patch("web.backend.execute_query", return_value=[]),
            patch("web.backend.read_job_history", return_value=[]),
        ):
            resp = client.get("/admin")
        assert resp.status_code in (302, 401)

    def test_admin_accessible_when_logged_in(self, admin_client):
        with (
            patch("web.backend.execute_query", return_value=[]),
            patch("web.backend.read_job_history", return_value=[]),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = admin_client.get("/admin")
        assert resp.status_code == 200

    def test_login_get_renders_form(self, client, app):
        app.config["TESTING"] = True
        import os

        with (
            patch.dict(os.environ, {"ADMIN_PASSWORD": "secret"}),
            patch("web.backend.ADMIN_PASSWORD", "secret"),
            patch("web.backend.render_template", return_value="<html/>") as mock_rt,
        ):
            resp = client.get("/admin/login")
        assert resp.status_code == 200
        mock_rt.assert_called_with("admin_login.html", error=None)

    def test_login_post_correct_password(self, client, app):
        with (
            patch("web.backend.ADMIN_PASSWORD", "correctpass"),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            # First GET to establish session with csrf_token
            client.get("/admin/login")
            with client.session_transaction() as sess:
                csrf = sess.get("csrf_token", "")
            resp = client.post(
                "/admin/login",
                data={
                    "password": "correctpass",
                    "csrf_token": csrf,
                },
            )
        assert resp.status_code == 302

    def test_login_post_wrong_password_stays_on_page(self, client):
        with (
            patch("web.backend.ADMIN_PASSWORD", "correctpass"),
            patch("web.backend.render_template", return_value="<html/>") as mock_rt,
        ):
            client.get("/admin/login")
            with client.session_transaction() as sess:
                csrf = sess.get("csrf_token", "")
            client.post("/admin/login", data={"password": "wrong", "csrf_token": csrf})
        # render_template should have been called with an error
        calls = [c for c in mock_rt.call_args_list if c.args[0] == "admin_login.html"]
        assert any(c.kwargs.get("error") for c in calls)

    def test_logout_clears_session(self, admin_client):
        resp = admin_client.get("/admin/logout")
        assert resp.status_code == 302
        with admin_client.session_transaction() as sess:
            assert "admin" not in sess


# ── CSRF protection ───────────────────────────────────────────────────────────


class TestCsrfProtection:
    def test_post_without_csrf_returns_403(self, admin_client):
        resp = admin_client.post(
            "/api/job/stop/scrape_leon",
            json={},
            headers={"Content-Type": "application/json"},
            # Deliberately no X-CSRF-Token header
        )
        assert resp.status_code == 403

    def test_post_with_correct_csrf_passes(self, admin_client):
        with patch("web.backend.stop_job", return_value=True):
            resp = post_json(admin_client, "/api/job/stop/scrape_leon", {})
        assert resp.status_code == 200

    def test_post_with_wrong_csrf_returns_403(self, admin_client):
        resp = admin_client.post(
            "/api/job/stop/scrape_leon",
            json={"csrf_token": "wrong-token"},
            headers={"Content-Type": "application/json", "X-CSRF-Token": "wrong-token"},
        )
        assert resp.status_code == 403

    def test_get_requests_bypass_csrf(self, admin_client):
        with patch(
            "web.backend.get_job", return_value={"running": False, "progress": 0, "message": ""}
        ):
            resp = admin_client.get("/api/job/scrape_leon")
        assert resp.status_code == 200


# ── Job API ───────────────────────────────────────────────────────────────────


class TestJobApi:
    def test_job_status_unknown_name_returns_400(self, admin_client):
        resp = admin_client.get("/api/job/not_a_real_job")
        assert resp.status_code == 400

    def test_job_status_not_started_returns_not_running(self, admin_client):
        with patch("web.backend.get_job", return_value=None):
            resp = admin_client.get("/api/job/scrape_leon")
        data = resp.get_json()
        assert data["running"] is False

    def test_job_status_running(self, admin_client):
        with patch(
            "web.backend.get_job",
            return_value={"running": True, "progress": 50, "message": "halfway"},
        ):
            resp = admin_client.get("/api/job/scrape_leon")
        data = resp.get_json()
        assert data["running"] is True
        assert data["progress"] == 50

    def test_stop_running_job(self, admin_client):
        with patch("web.backend.stop_job", return_value=True):
            resp = post_json(admin_client, "/api/job/stop/scrape_leon", {})
        data = resp.get_json()
        assert data["ok"] is True
        assert "stop signal sent" in data["message"]

    def test_stop_not_running_job(self, admin_client):
        with patch("web.backend.stop_job", return_value=False):
            resp = post_json(admin_client, "/api/job/stop/scrape_leon", {})
        data = resp.get_json()
        assert data["ok"] is False

    def test_job_history_returns_list(self, admin_client):
        history = [{"job": "scrape_leon", "status": "done", "message": "ok", "at": "2024-01-01"}]
        with patch("web.backend.read_job_history", return_value=history):
            resp = admin_client.get("/api/job_history")
        assert resp.status_code == 200
        assert isinstance(resp.get_json(), list)


# ── Admin POST actions ─────────────────────────────────────────────────────────


class TestAdminPost:
    def test_unknown_action_returns_400(self, admin_client):
        resp = post_json(admin_client, "/admin", {"action": "not_real"})
        assert resp.status_code == 400

    def test_get_manga_invalid_offset_returns_400(self, admin_client):
        resp = post_json(admin_client, "/admin", {"action": "get_manga", "offset": "abc"})
        assert resp.status_code == 400

    def test_scrape_leon_no_range_returns_400(self, admin_client):
        resp = post_json(admin_client, "/admin", {"action": "scrape_leon"})
        assert resp.status_code == 400
        assert "range" in resp.get_json()["message"].lower()

    def test_scrape_leon_valid_range_starts_job(self, admin_client):
        with patch("web.backend.start_job", return_value=(True, 200, "scrape_leon started")):
            resp = post_json(admin_client, "/admin", {"action": "scrape_leon", "range": "1-10"})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

    def test_scrape_broward_valid_range_starts_job(self, admin_client):
        with patch("web.backend.start_job", return_value=(True, 200, "scrape_broward started")):
            resp = post_json(admin_client, "/admin", {"action": "scrape_broward", "range": "1-10"})
        assert resp.status_code == 200

    def test_scrape_already_running_returns_409(self, admin_client):
        with patch("web.backend.start_job", return_value=(False, 409, "already running")):
            resp = post_json(admin_client, "/admin", {"action": "scrape_leon", "range": "1-10"})
        assert resp.status_code == 409


# ── Missing titles ────────────────────────────────────────────────────────────


class TestMissingTitles:
    def test_missing_titles_structure(self, admin_client):
        with (
            patch("web.backend._missing_manga_ids", return_value=[1, 2, 3]),
            patch("web.backend._titles_count", return_value=100),
        ):
            resp = admin_client.get("/api/missing_titles")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "count" in data
        assert "broward_count" in data
        assert "total_titles" in data


# ── Delete title ──────────────────────────────────────────────────────────────


class TestDeleteTitle:
    def test_delete_without_manga_id_returns_400(self, admin_client):
        resp = post_json(admin_client, "/api/delete_title_results", {})
        assert resp.status_code == 400

    def test_delete_with_manga_id_returns_ok(self, admin_client):
        with patch("web.backend.execute_update", return_value=1):
            resp = post_json(admin_client, "/api/delete_title_results", {"manga_id": 1})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

    def test_delete_db_error_returns_500(self, admin_client):
        with patch("web.backend.execute_update", side_effect=Exception("db error")):
            resp = post_json(admin_client, "/api/delete_title_results", {"manga_id": 1})
        assert resp.status_code == 500


# ── MAL endpoints ─────────────────────────────────────────────────────────────


class TestMalEndpoints:
    def test_mal_mangalist_without_token_returns_503(self, client):
        import os

        with patch.dict(os.environ, {"MAL_ACCESS_TOKEN": ""}):
            resp = client.get("/api/mal/mangalist")
        assert resp.status_code == 503

    def test_mal_mangalist_with_token_starts_job(self, client):
        import os

        with (
            patch.dict(os.environ, {"MAL_ACCESS_TOKEN": "fake_token"}),
            patch("threading.Thread") as mock_thread,
        ):
            mock_thread.return_value = MagicMock()
            resp = client.get("/api/mal/mangalist")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "job_id" in data

    def test_mal_status_not_found(self, client):
        resp = client.get("/api/mal/mangalist/status/nonexistent-id")
        assert resp.status_code == 404

    def test_mal_set_filter_requires_csrf(self, client):
        resp = client.post(
            "/api/mal/set_filter",
            json={"filters": {"reading": "include"}},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 403

    def test_mal_set_filter_with_csrf_saves_session(self, client):
        with client.session_transaction() as sess:
            sess["csrf_token"] = "test-csrf-token"
        resp = post_json(
            client,
            "/api/mal/set_filter",
            {
                "filters": {
                    "reading": "include",
                    "completed": "",
                    "on_hold": "",
                    "dropped": "",
                    "plan_to_read": "",
                }
            },
        )
        assert resp.status_code == 200
        with client.session_transaction() as sess:
            assert sess.get("mal_filters", {}).get("reading") == "include"

    def test_mal_set_filter_rejects_invalid_filter_values(self, client):
        with client.session_transaction() as sess:
            sess["csrf_token"] = "test-csrf-token"
        resp = post_json(
            client,
            "/api/mal/set_filter",
            {
                "filters": {
                    "reading": "HACKED_VALUE",
                    "completed": "",
                    "on_hold": "",
                    "dropped": "",
                    "plan_to_read": "",
                }
            },
        )
        assert resp.status_code == 200
        with client.session_transaction() as sess:
            # Invalid value should be stripped, not stored
            saved = sess.get("mal_filters", {})
            assert "reading" not in saved or saved.get("reading") != "HACKED_VALUE"

    def test_mal_clear_filter_clears_session(self, client):
        with client.session_transaction() as sess:
            sess["mal_data"] = {"1": {"status": "reading"}}
            sess["mal_filters"] = {"reading": "include"}
            sess["csrf_token"] = "test-csrf-token"
        post_json(client, "/api/mal/clear_filter", {})
        with client.session_transaction() as sess:
            assert "mal_data" not in sess
            assert "mal_filters" not in sess


# ── Security headers ──────────────────────────────────────────────────────────


class TestSecurityHeaders:
    def test_x_content_type_options_present(self, client):
        with (
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = client.get("/")
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"

    def test_x_frame_options_deny(self, client):
        with (
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = client.get("/")
        assert resp.headers.get("X-Frame-Options") == "DENY"

    def test_referrer_policy_present(self, client):
        with (
            patch("web.backend.get_library_ids", return_value=(1, 2)),
            patch("web.backend.render_template", return_value="<html/>"),
        ):
            resp = client.get("/")
        assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
