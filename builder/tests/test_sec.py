from divkit_builder import sec


def test_ticker_cik_map_parses_fixture(monkeypatch):
    sample = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    monkeypatch.setattr(sec, "_get_json", lambda url: sample)
    m = sec.ticker_cik_map()
    assert m["AAPL"] == 320193


def test_user_agent_bare_form():
    ua = sec.user_agent()
    assert ua.startswith("divkit ")
    assert "(" not in ua
    assert "http" not in ua
