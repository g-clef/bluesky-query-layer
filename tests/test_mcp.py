import json

import pytest
from src.exceptions import QueryError
from src.mcp import handle_mcp_request


class MockProxy:
    def __init__(self, rows=None, raise_exc=None):
        self._rows = rows or []
        self._raise = raise_exc

    def query(self, sql: str) -> list[dict]:
        if self._raise:
            raise self._raise
        return self._rows


def _req(method, params=None, req_id=1):
    return {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}


def test_initialize_returns_protocol_version():
    resp = handle_mcp_request(_req("initialize"), MockProxy())
    assert resp["result"]["protocolVersion"] == "2024-11-05"
    assert "tools" in resp["result"]["capabilities"]
    assert resp["result"]["serverInfo"]["name"] == "bluesky-query"


def test_list_tools_returns_single_tool():
    resp = handle_mcp_request(_req("tools/list"), MockProxy())
    tools = resp["result"]["tools"]
    assert len(tools) == 1
    assert tools[0]["name"] == "query_posts"
    assert "sql" in tools[0]["inputSchema"]["properties"]
    assert tools[0]["inputSchema"]["required"] == ["sql"]


def test_list_tools_description_contains_column_names():
    resp = handle_mcp_request(_req("tools/list"), MockProxy())
    desc = resp["result"]["tools"][0]["description"]
    for col in ("did", "record_text", "collection_timestamp", "posts"):
        assert col in desc, f"Expected '{col}' in tool description"


def test_call_tool_returns_rows_as_json():
    rows = [{"did": "did:plc:abc123", "record_text": "hello"}]
    resp = handle_mcp_request(
        _req(
            "tools/call",
            {"name": "query_posts", "arguments": {"sql": "SELECT * FROM posts"}},
        ),
        MockProxy(rows=rows),
    )
    assert resp["result"]["isError"] is False
    returned = json.loads(resp["result"]["content"][0]["text"])
    assert returned == rows


def test_call_tool_unknown_tool_returns_invalid_params():
    resp = handle_mcp_request(
        _req("tools/call", {"name": "nonexistent", "arguments": {}}),
        MockProxy(),
    )
    assert "error" in resp
    assert resp["error"]["code"] == -32602


def test_call_tool_missing_sql_returns_invalid_params():
    resp = handle_mcp_request(
        _req("tools/call", {"name": "query_posts", "arguments": {}}),
        MockProxy(),
    )
    assert "error" in resp
    assert resp["error"]["code"] == -32602


def test_call_tool_query_error_returns_is_error_true():
    resp = handle_mcp_request(
        _req(
            "tools/call",
            {"name": "query_posts", "arguments": {"sql": "INVALID"}},
        ),
        MockProxy(raise_exc=QueryError("Parser error: syntax error at INVALID")),
    )
    assert resp["result"]["isError"] is True
    assert "Parser error" in resp["result"]["content"][0]["text"]


def test_unknown_method_returns_method_not_found():
    resp = handle_mcp_request(_req("notifications/unknown"), MockProxy())
    assert "error" in resp
    assert resp["error"]["code"] == -32601


def test_missing_method_key_returns_error():
    # A body with no "method" key is an invalid request
    resp = handle_mcp_request({"jsonrpc": "2.0", "id": 1}, MockProxy())
    assert "error" in resp


def test_missing_method_key_returns_invalid_request_code():
    # JSON-RPC 2.0 §5: missing required field → Invalid Request (-32600)
    resp = handle_mcp_request({"jsonrpc": "2.0", "id": 1}, MockProxy())
    assert resp["error"]["code"] == -32600


def test_call_tool_with_no_params_key_returns_error():
    # tools/call with params key absent entirely should not crash
    resp = handle_mcp_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call"},
        MockProxy(),
    )
    assert "error" in resp


def test_call_tool_whitespace_sql_raises_error():
    # Whitespace-only sql is falsy when stripped — should return an error
    resp = handle_mcp_request(
        _req(
            "tools/call",
            {"name": "query_posts", "arguments": {"sql": "   "}},
        ),
        MockProxy(),
    )
    assert "error" in resp