"""Headless browser check of the three Grafana dashboards in env.md. Needs Grafana up with data loaded."""

from __future__ import annotations

import os

import pytest

from tests.conftest import ROOT

pytestmark = pytest.mark.e2e

GRAFANA = f"http://127.0.0.1:{os.environ.get('GRAFANA_PORT', '3000')}"
SHOTS = ROOT / "docs/screenshots"
PANEL = '[data-testid^="data-testid Panel header"]'


@pytest.fixture(scope="module")
def page():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1000})
        page.goto(f"{GRAFANA}/login")
        page.fill('input[name="user"]', "admin")
        page.fill('input[name="password"]', os.environ["GRAFANA_ADMIN_PASSWORD"])
        page.click('button[type="submit"]')
        page.wait_for_url(lambda url: "/login" not in url)
        yield page
        browser.close()


@pytest.mark.parametrize("uid", ["sales", "pipeline-health", "quality"])
def test_dashboard_loads_with_data(page, uid):
    page.goto(f"{GRAFANA}/d/{uid}?orgId=1&kiosk")
    page.wait_for_selector(PANEL, timeout=30_000)
    page.wait_for_load_state("networkidle")
    SHOTS.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(SHOTS / f"{uid}.png"), full_page=True)
    assert page.locator(PANEL).count() > 0
    assert page.locator('[data-testid="data-testid Panel status error"]').count() == 0, "panel error"
    assert page.get_by_text("No data", exact=True).count() == 0, "panel without data"
