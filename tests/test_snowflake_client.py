import pytest

from app import snowflake_client as sc


class _Cursor:
    def __init__(self, fail_with=None):
        self.fail_with = fail_with
        self.executed = []
        self.description = [("X",)]

    def execute(self, sql, params=None, timeout=None):
        self.executed.append((sql, params, timeout))
        if self.fail_with:
            exc, self.fail_with = self.fail_with, None
            raise exc

    def fetchall(self):
        return [(1,)]

    def fetchone(self):
        return (1,)

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def is_closed(self):
        return self.closed

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class _Expired(Exception):
    errno = 390112


def test_query_passes_statement_timeout_and_returns_rows(monkeypatch):
    cur = _Cursor()
    monkeypatch.setattr(sc, "_connection", _Conn(cur))
    rows = sc.query("select 1")
    assert rows == [{"X": 1}]
    assert cur.executed[0][2] == sc.QUERY_TIMEOUT_SECONDS


def test_query_reconnects_once_on_session_expiry(monkeypatch):
    first = _Conn(_Cursor(fail_with=_Expired("expired")))
    second = _Conn(_Cursor())
    conns = iter([second])
    monkeypatch.setattr(sc, "_connection", first)
    monkeypatch.setattr(sc, "_connect", lambda: next(conns))
    assert sc.query("select 1") == [{"X": 1}]
    assert first.closed is True


def test_query_does_not_retry_sql_errors(monkeypatch):
    class Programming(Exception):
        errno = 1003
    monkeypatch.setattr(sc, "_connection", _Conn(_Cursor(fail_with=Programming("bad sql"))))
    monkeypatch.setattr(sc, "_connect", lambda: pytest.fail("must not reconnect on SQL errors"))
    with pytest.raises(Programming):
        sc.query("select bad")


def test_ping_uses_per_statement_timeout_not_session_alter(monkeypatch):
    cur = _Cursor()
    monkeypatch.setattr(sc, "_connection", _Conn(cur))
    assert sc.ping(timeout_seconds=5) is True
    assert all("ALTER SESSION" not in sql for sql, _, _ in cur.executed)
    assert cur.executed[0][2] == 5


def test_ping_reconnects_on_expired_session(monkeypatch):
    first = _Conn(_Cursor(fail_with=_Expired("expired")))
    monkeypatch.setattr(sc, "_connection", first)
    monkeypatch.setattr(sc, "_connect", lambda: _Conn(_Cursor()))
    assert sc.ping() is True


def test_ping_reports_last_known_health_when_busy(monkeypatch):
    monkeypatch.setattr(sc, "_last_healthy", False)
    assert sc._lock.acquire()
    try:
        assert sc.ping(timeout_seconds=1) is False
    finally:
        sc._lock.release()


def test_query_marks_unhealthy_on_connect_failure(monkeypatch):
    monkeypatch.setattr(sc, "_connection", None)
    monkeypatch.setattr(sc, "_connect", lambda: (_ for _ in ()).throw(RuntimeError("down")))
    with pytest.raises(RuntimeError):
        sc.query("select 1")
    assert sc._last_healthy is False
