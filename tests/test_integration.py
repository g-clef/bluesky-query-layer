# tests/test_integration.py
import json

import duckdb
import pytest
from fastapi.testclient import TestClient

from src.exceptions import QueryError
from src.service import app, get_actor


class LocalQueryProxy:
    """Direct DuckDB connection — no Ray required."""

    def __init__(self, parquet_glob: str) -> None:
        self.conn = duckdb.connect()
        self.conn.execute(
            f"CREATE OR REPLACE VIEW posts AS "
            f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)"
        )

    def query(self, sql: str) -> list[dict]:
        if not sql.lstrip().lower().startswith("select"):
            raise QueryError("Only SELECT queries are allowed")
        try:
            result = self.conn.execute(sql)
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]
        except duckdb.Error as e:
            raise QueryError(str(e)) from e


@pytest.fixture
def integration_client(fixture_parquet_dir):
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    proxy = LocalQueryProxy(glob)
    app.dependency_overrides[get_actor] = lambda: proxy
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_rest_full_scan(integration_client):
    resp = integration_client.post("/query", json={"sql": "SELECT * FROM posts"})
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 5


def test_rest_filter_by_did(integration_client):
    resp = integration_client.post(
        "/query",
        json={"sql": "SELECT * FROM posts WHERE did = 'did:plc:abc123'"},
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 3
    assert all(r["did"] == "did:plc:abc123" for r in rows)


def test_rest_aggregation(integration_client):
    resp = integration_client.post(
        "/query",
        json={
            "sql": (
                "SELECT did, COUNT(*) as cnt FROM posts "
                "GROUP BY did ORDER BY cnt DESC"
            )
        },
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert rows[0]["did"] == "did:plc:abc123"
    assert rows[0]["cnt"] == 3


def test_rest_hive_partition_filter(integration_client):
    resp = integration_client.post(
        "/query",
        json={"sql": "SELECT * FROM posts WHERE year = 2025 AND month = 1"},
    )
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 5


def test_rest_invalid_sql_returns_400(integration_client):
    resp = integration_client.post(
        "/query", json={"sql": "SELECT * FROM nonexistent_table_xyz"}
    )
    assert resp.status_code == 400


def test_mcp_end_to_end_call_tool(integration_client):
    resp = integration_client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "query_posts",
                "arguments": {
                    "sql": "SELECT did, record_text FROM posts LIMIT 2"
                },
            },
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"]["isError"] is False
    rows = json.loads(body["result"]["content"][0]["text"])
    assert len(rows) == 2
    assert "did" in rows[0]
    assert "record_text" in rows[0]


def test_mcp_initialize_end_to_end(integration_client):
    resp = integration_client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}},
    )
    assert resp.status_code == 200
    assert resp.json()["result"]["protocolVersion"] == "2024-11-05"