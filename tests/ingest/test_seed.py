"""seed.main must enter the connection context only once (psycopg2 rejects re-entry)."""

from ingest import seed


class Conn:
    def __init__(self):
        self.depth = 0
        self.closed = False

    def __enter__(self):
        self.depth += 1
        assert self.depth == 1, "the connection cannot be re-entered recursively"
        return self

    def __exit__(self, *exc):
        self.depth -= 1

    def cursor(self):
        return Cur()

    def close(self):
        self.closed = True


class Cur:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


def test_main_enters_the_connection_once(monkeypatch):
    conn = Conn()
    monkeypatch.setenv("WAREHOUSE_URL", "postgresql://unused")
    monkeypatch.setattr(seed.psycopg2, "connect", lambda url: conn)
    monkeypatch.setattr(seed, "execute_values", lambda cur, sql, values: None)
    seed.main()
    assert conn.closed
