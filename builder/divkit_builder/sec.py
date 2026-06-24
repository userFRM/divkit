"""SEC EDGAR HTTP session and ticker-CIK utilities."""

from __future__ import annotations

import os
import time

import httpx

# ---------------------------------------------------------------------------
# User-Agent — bare form required by SEC WAF (no parentheses, no URL)
# ---------------------------------------------------------------------------
CONTACT_EMAIL: str = os.environ.get(
    "DIVKIT_CONTACT_EMAIL", "divkit-bot@users.noreply.github.com"
)


def user_agent() -> str:
    """Return the ``divkit <email>`` User-Agent string.

    The SEC WAF 403s any UA containing parentheses or a URL; keep this bare.
    """
    return f"divkit {CONTACT_EMAIL}"


# ---------------------------------------------------------------------------
# Rate-limiter state: no more than 10 requests per second
# ---------------------------------------------------------------------------
_last_request_time: float = 0.0
_MIN_INTERVAL: float = 0.1  # seconds — 10 req/s ceiling


def _throttle() -> None:
    """Block until at least ``_MIN_INTERVAL`` seconds have passed since the last call."""
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request_time = time.monotonic()


# ---------------------------------------------------------------------------
# Shared client
# ---------------------------------------------------------------------------
_client: httpx.Client | None = None


def session() -> httpx.Client:
    """Return (or create) the shared ``httpx.Client`` configured for the SEC.

    * HTTP/2 enabled.
    * ``User-Agent`` header set to the bare ``divkit <email>`` form.
    * Requests are throttled to ≤ 10/s via :func:`_throttle`.
    """
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(
            http2=True,
            headers={"User-Agent": user_agent()},
            timeout=30.0,
        )
    return _client


# ---------------------------------------------------------------------------
# Low-level JSON fetch
# ---------------------------------------------------------------------------
def _get_json(url: str) -> dict:
    """Fetch *url* as JSON, respecting the ≤10 req/s rate limit."""
    _throttle()
    resp = session().get(url)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Ticker ↔ CIK maps
# ---------------------------------------------------------------------------
_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def ticker_cik_map() -> dict[str, int]:
    """Return ``{TICKER: cik_int}`` for all SEC-registered companies.

    Source: ``https://www.sec.gov/files/company_tickers.json`` — a dict-of-dicts
    where each value has ``cik_str`` (int), ``ticker`` (str), and ``title`` (str).
    Tickers are upper-cased; CIKs are returned as plain ``int``.
    """
    raw: dict = _get_json(_TICKERS_URL)
    return {v["ticker"].upper(): int(v["cik_str"]) for v in raw.values()}


def cik_ticker_map() -> dict[int, str]:
    """Return ``{cik_int: TICKER}`` — inverse of :func:`ticker_cik_map`.

    When multiple tickers share a CIK, the first one encountered wins.
    """
    result: dict[int, str] = {}
    for ticker, cik in ticker_cik_map().items():
        result.setdefault(cik, ticker)
    return result
