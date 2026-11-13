"""ssp.export.submittable.clickhouse_client, query_parquet, stream_parquet
(docs/design/clickhouse-timeouts.md), against a stub ClickHouse HTTP server
on localhost: a query kept alive by progress headers outlives the client's
read timeout; a silent one doesn't; an error after the headers (the HTTP
status is then already 200) raises, never comes back as data."""
import argparse
import io
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp.export import submittable as S

TABLE = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "x": pa.array([0.5, 1.5, 2.5])})


def parquet_bytes(table=TABLE):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _str(s):
    s = s.encode()
    return _varint(len(s)) + s


def native_settings(rows):
    """``system.settings`` (name, value, readonly) in ClickHouse's Native
    format, as the client reads it while connecting."""
    out = _varint(3) + _varint(len(rows))
    out += _str("name") + _str("String") + b"".join(_str(r[0]) for r in rows)
    out += _str("value") + _str("String") + b"".join(_str(r[1]) for r in rows)
    out += _str("readonly") + _str("UInt8") + bytes(r[2] for r in rows)
    return out


SETTINGS = [(name, value, 0) for name, value in [
    ("readonly", "0"), ("max_execution_time", "0"), ("send_progress_in_http_headers", "0"),
    ("http_headers_progress_interval_ms", "100"), ("cancel_http_readonly_queries_on_client_close", "0"),
    ("enable_http_compression", "0"), ("wait_end_of_query", "0")]]


class Stub(BaseHTTPRequestHandler):
    """Answers the client's connection queries; a query containing
    ``SELECT 'case'`` gets the server's ``cases[case]``:
    ``(progress_s, body_parts, extra_headers)``: progress headers for
    ``progress_s`` (at the request's http_headers_progress_interval_ms, if
    it asks for progress), or silence, then the body."""
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        params = {k: v[-1] for k, v in parse_qs(urlparse(self.path).query).items()}
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        text = body.decode(errors="replace") + params.get("query", "")
        self.server.requests.append((params, text))
        if "SELECT version(), timezone()" in text:
            return self._reply(b"25.8.1.1\tUTC\n")
        if "FROM system.settings" in text:
            return self._reply(native_settings(SETTINGS))
        if "SELECT 1 AS check" in text:
            return self._reply(b"Code: 1. DB::Exception: no", status=500)
        case = next(c for c in self.server.cases if f"SELECT '{c}'" in text)
        progress_s, parts, extra = self.server.cases[case]
        try:
            self._slow(params, progress_s, parts, extra)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _reply(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _slow(self, params, progress_s, parts, extra):
        w = self.wfile
        w.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\n")
        w.flush()
        t_end = time.monotonic() + progress_s
        if params.get("send_progress_in_http_headers") == "1":
            interval = int(params.get("http_headers_progress_interval_ms", 100_000)) / 1e3
            while time.monotonic() < t_end:
                time.sleep(min(interval, max(0.0, t_end - time.monotonic())))
                w.write(b'X-ClickHouse-Progress: {"read_rows":"1"}\r\n')
                w.flush()
                self.server.progress += 1
        else:
            time.sleep(progress_s)
        for h in extra:
            w.write(h.encode() + b"\r\n")
        w.write(b"\r\n")                      # no Content-Length: the body ends at close
        for p in parts:
            w.write(p)
            w.flush()
        self.close_connection = True


@pytest.fixture
def stub(monkeypatch):
    for v in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("SSP_CH_USER", "u")
    monkeypatch.setenv("SSP_CH_PASSWORD", "p")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    server.daemon_threads = True
    server.cases, server.requests, server.progress = {}, [], 0
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


def client(server, **kw):
    # a 1 s client read timeout (1 + 0 s), progress every 0.2 s
    kw = dict(dict(max_execution_time=1, margin_s=0, progress_interval_ms=200), **kw)
    return S.clickhouse_client("127.0.0.1", server.server_address[1], "ssp", **kw)


def query_params(server, case):
    return next(p for p, text in server.requests if f"SELECT '{case}'" in text)


def test_progress_headers_keep_a_long_query_alive(stub):
    stub.cases["slow"] = (2.5, [parquet_bytes()], [])     # 2.5 s > the 1 s read timeout
    c = client(stub)
    assert S.query_parquet(c, "SELECT 'slow'").equals(TABLE)
    assert stub.progress >= 5
    # the server is asked to stop first, and to cancel the query if we go away
    p = query_params(stub, "slow")
    assert p["max_execution_time"] == "1" and p["readonly"] == "1"
    assert p["cancel_http_readonly_queries_on_client_close"] == "1"


def test_stream_with_progress_headers(stub, tmp_path):
    stub.cases["slow"] = (2.5, [parquet_bytes()], [])
    S.stream_parquet(client(stub), "SELECT 'slow'", tmp_path / "out.parquet")
    assert pq.read_table(tmp_path / "out.parquet").equals(TABLE)


def test_silent_query_times_out(stub):
    stub.cases["silent"] = (2.5, [parquet_bytes()], [])
    c = client(stub)
    c.set_client_setting("send_progress_in_http_headers", "0")
    getattr(c, "_backend", c).send_progress = False
    t0 = time.monotonic()
    with pytest.raises(Exception, match="(?i)timed? ?out"):
        S.query_parquet(c, "SELECT 'silent'")
    assert time.monotonic() - t0 < 2.4


def test_client_timeout_follows_max_execution_time(stub):
    # silent for 2.5 s: within a 3 s client timeout (2 + 1) it completes
    stub.cases["silent"] = (2.5, [parquet_bytes()], [])
    c = client(stub, max_execution_time=2, margin_s=1)
    getattr(c, "_backend", c).send_progress = False
    c.set_client_setting("send_progress_in_http_headers", "0")
    assert S.query_parquet(c, "SELECT 'silent'").equals(TABLE)


OLD_ERROR = b"Code: 159. DB::Exception: Timeout exceeded: elapsed 5.0 seconds. (TIMEOUT_EXCEEDED)\n"
TAG = "abcdefgh12345678"
NEW_ERROR = (f"__exception__\r\n{TAG}\r\nCode: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)\n"
             f"68 {TAG}\r\n__exception__\r\n").encode()
HALF = parquet_bytes()[:40]

ERRORS = {
    "error_only": ([OLD_ERROR], []),
    "error_after_data": ([HALF, OLD_ERROR], []),
    "tagged_error": ([HALF, NEW_ERROR], [f"X-ClickHouse-Exception-Tag: {TAG}"]),
    "truncated": ([HALF], []),
    "empty": ([], []),
    "error_after_parquet": ([parquet_bytes(), OLD_ERROR], []),
    # ClickHouse 26.8: a chunked body that stops before its last chunk
    "broken_chunked": ([b"%x\r\n%s\r\n" % (len(HALF), HALF)], ["Transfer-Encoding: chunked"]),
}


@pytest.mark.parametrize("case", sorted(ERRORS))
@pytest.mark.parametrize("how", ["query", "stream"])
def test_error_after_headers_raises(stub, tmp_path, case, how):
    parts, extra = ERRORS[case]
    stub.cases[case] = (0.5, parts, extra)
    c = client(stub)
    match = "TIMEOUT_EXCEEDED" if OLD_ERROR in parts or NEW_ERROR in parts else "not a complete Parquet"
    if case == "broken_chunked":
        # clickhouse_connect raises for raw_query itself (a urllib3 error)
        error, match = ((Exception, "IncompleteRead") if how == "query"
                        else (S.ClickHouseResultError, "broke off"))
    else:
        error = S.ClickHouseResultError
    with pytest.raises(error, match=match):
        if how == "query":
            S.query_parquet(c, f"SELECT '{case}'")
        else:
            S.stream_parquet(c, f"SELECT '{case}'", tmp_path / "out.parquet")


def test_exception_code_header_raises(stub, tmp_path):
    # an error before the body, after progress: status 200 + the code
    stub.cases["coded"] = (0.5, [OLD_ERROR], ["X-ClickHouse-Exception-Code: 159"])
    with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
        S.query_parquet(client(stub), "SELECT 'coded'")
    with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
        S.stream_parquet(client(stub), "SELECT 'coded'", tmp_path / "out.parquet")


def test_run_queries_through_stub(stub):
    stub.cases["q"] = (0.5, [parquet_bytes()], [])
    tasks = [(k, "SELECT 'q' FROM t WHERE id IN (SELECT q FROM q)", None, dict(q=[1, 2])) for k in range(3)]
    out = S.run_queries(tasks, "127.0.0.1", stub.server_address[1], "ssp", None, workers=2)
    assert [k for k, _ in out] == [0, 1, 2] and all(t.equals(TABLE) for _, t in out)


def test_run_queries_error_raises(stub):
    stub.cases["bad"] = (0.5, [HALF, OLD_ERROR], [])
    tasks = [(0, "SELECT 'bad'", None, dict(q=[1]))]
    with pytest.raises(S.ClickHouseResultError, match="TIMEOUT_EXCEEDED"):
        S.run_queries(tasks, "127.0.0.1", stub.server_address[1], "ssp", None, workers=1)


def test_export_ppdb_error_is_an_extract_error(stub, tmp_path, monkeypatch):
    from ssp import sso_inputs as M
    stub.cases["ppdb"] = (0.5, [HALF, OLD_ERROR], [])
    monkeypatch.setattr(M, "PPDB_SQL", "SELECT 'ppdb'")
    monkeypatch.setattr(M, "tap_url", lambda args: None)
    args = argparse.Namespace(ch_host="127.0.0.1", ch_port=stub.server_address[1], ch_user=None)
    with pytest.raises(M.ExtractError, match="ppdb_dia_sources: .*TIMEOUT_EXCEEDED"):
        M.export_ppdb(tmp_path / "ppdb.parquet", args)
