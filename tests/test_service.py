import pytest
from fastapi.testclient import TestClient

from src.exceptions import QueryError, QueryTimeoutError, S3Error
from src.service import app, get_actor


class MockProxy:
    def __init__(self, rows=None, raise_exc=None, ping_result=True):
        self._rows = rows or []
        self._raise = raise_exc
        self._ping_result = ping_result

    def query(self, sql: str) -> list[dict]:
        if self._raise:
            raise self._raise
        return self._rows

    def ping(self) -> bool:
        return self._ping_result


@pytest.fixture
def client():
    app.dependency_overrides[get_actor] = lambda: MockProxy(
        rows=[{"did": "did:plc:abc123", "record_text": "hello"}]
    )
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_400():
    app.dependency_overrides[get_actor] = lambda: MockProxy(
        raise_exc=QueryError("Parser error: syntax error")
    )
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_408():
    app.dependency_overrides[get_actor] = lambda: MockProxy(
        raise_exc=QueryTimeoutError("Query exceeded 300s timeout")
    )
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_503():
    app.dependency_overrides[get_actor] = lambda: MockProxy(
        raise_exc=S3Error("Connection refused to MinIO")
    )
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --- /health ---

def test_health_returns_200(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_health_returns_503_when_actor_not_initialized():
    app.dependency_overrides.clear()
    with TestClient(app) as c:
        resp = c.get("/health")
    assert resp.status_code == 503


def test_health_returns_503_when_actor_unreachable():
    app.dependency_overrides[get_actor] = lambda: MockProxy(ping_result=False)
    with TestClient(app) as c:
        resp = c.get("/health")
    app.dependency_overrides.clear()
    assert resp.status_code == 503


# --- POST /query ---

def test_query_returns_rows(client):
    resp = client.post("/query", json={"sql": "SELECT * FROM posts"})
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["did"] == "did:plc:abc123"


def test_query_invalid_sql_returns_400(client_400):
    resp = client_400.post("/query", json={"sql": "INVALID"})
    assert resp.status_code == 400
    assert "Parser error" in resp.json()["detail"]


def test_query_timeout_returns_408(client_408):
    resp = client_408.post("/query", json={"sql": "SELECT * FROM posts"})
    assert resp.status_code == 408


def test_query_s3_error_returns_503(client_503):
    resp = client_503.post("/query", json={"sql": "SELECT * FROM posts"})
    assert resp.status_code == 503


def test_query_missing_sql_returns_422(client):
    resp = client.post("/query", json={})
    assert resp.status_code == 422


# --- POST /mcp ---

def test_mcp_initialize(client):
    resp = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}},
    )
    assert resp.status_code == 200
    assert resp.json()["result"]["serverInfo"]["name"] == "bluesky-query"


def test_mcp_list_tools(client):
    resp = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )
    assert resp.status_code == 200
    assert resp.json()["result"]["tools"][0]["name"] == "query_posts"


def test_mcp_call_tool_success(client):
    resp = client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "query_posts",
                "arguments": {"sql": "SELECT * FROM posts"},
            },
        },
    )
    assert resp.status_code == 200
    assert resp.json()["result"]["isError"] is False


def test_mcp_query_error_is_mcp_level_error(client_400):
    resp = client_400.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "query_posts",
                "arguments": {"sql": "INVALID"},
            },
        },
    )
    assert resp.status_code == 200  # MCP errors are 200 with isError=True in result
    assert resp.json()["result"]["isError"] is True


def test_actor_not_initialized_returns_503():
    # When get_actor is not overridden and _actor is None, expect 503
    app.dependency_overrides.clear()
    with TestClient(app) as c:
        resp = c.post("/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 503


def test_mcp_non_dict_body_returns_invalid_request():
    app.dependency_overrides[get_actor] = lambda: MockProxy()
    with TestClient(app) as c:
        resp = c.post("/mcp", content="[1,2,3]", headers={"content-type": "application/json"})
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    assert resp.json()["error"]["code"] == -32600


def test_mcp_invalid_json_body_returns_parse_error():
    app.dependency_overrides[get_actor] = lambda: MockProxy()
    with TestClient(app) as c:
        resp = c.post(
            "/mcp",
            content="{bad json",
            headers={"content-type": "application/json"},
        )
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == -32700


def test_lifespan_actor_creation_uses_get_if_exists(monkeypatch):
    """Fix for race condition: actor creation must pass get_if_exists=True."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from src.service import lifespan, app
    from src.actor import DuckDBQueryActor

    monkeypatch.setenv("RAY_ADDRESS", "auto")

    mock_handle = MagicMock()
    mock_options_ret = MagicMock()
    mock_options_ret.remote.return_value = mock_handle

    with patch("src.service.ray.init"), \
         patch("src.service.ray.shutdown"), \
         patch("src.service.ray.get_actor", side_effect=ValueError("not found")), \
         patch("src.service.DuckDBQueryActor.options", return_value=mock_options_ret) as mock_options:

        async def run():
            async with lifespan(app):
                pass

        asyncio.run(run())

        mock_options.assert_called_once_with(
            name="duckdb_query_actor",
            namespace="bluesky-query",
            lifetime="detached",
            get_if_exists=True,
        )