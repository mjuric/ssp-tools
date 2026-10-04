"""ssp.export.submittable.bypass_proxy: the ClickHouse host is kept out of
the environment's HTTP proxy (SDF's squid refuses it)."""
from ssp.export import submittable as S


def _env(monkeypatch, **kw):
    for v in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY"):
        monkeypatch.delenv(v, raising=False)
    for k, v in kw.items():
        monkeypatch.setenv(k, v)


def test_adds_host_when_proxied(monkeypatch):
    _env(monkeypatch, http_proxy="http://squid:3128", no_proxy="localhost,.slac.stanford.edu")
    S.bypass_proxy("172.24.10.116")
    import os
    assert os.environ["no_proxy"] == "172.24.10.116,localhost,.slac.stanford.edu"
    assert os.environ["NO_PROXY"] == "172.24.10.116"
    S.bypass_proxy("172.24.10.116")          # idempotent
    assert os.environ["no_proxy"].count("172.24.10.116") == 1


def test_noop_without_proxy_or_with_wildcard(monkeypatch):
    import os
    _env(monkeypatch)
    S.bypass_proxy("172.24.10.116")
    assert "no_proxy" not in os.environ and "NO_PROXY" not in os.environ
    _env(monkeypatch, HTTPS_PROXY="http://squid:3128", no_proxy="*", NO_PROXY="*")
    S.bypass_proxy("172.24.10.116")
    assert os.environ["no_proxy"] == "*" and os.environ["NO_PROXY"] == "*"


def test_default_host_is_the_kubernetes_server():
    assert S.DEFAULT_HOST == "172.24.10.116" and S.DEFAULT_PORT == 8123
