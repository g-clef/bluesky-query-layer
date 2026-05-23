import json

MCP_PROTOCOL_VERSION = "2024-11-05"

TOOL_SCHEMA = {
    "name": "query_posts",
    "description": (
        "Execute SQL against the Bluesky posts archive. "
        "Available table: posts. "
        "Columns: collection_timestamp (timestamp), event_timestamp (timestamp), "
        "did (varchar), event_type (varchar), collection (varchar), action (varchar), "
        "record_text (varchar), reply_parent (varchar), reply_root (varchar), "
        "embed_type (varchar), raw_event (varchar). "
        "Hive partitions usable in WHERE: year (int), month (int), day (int), hour (int). "
        "Example: SELECT did, COUNT(*) FROM posts WHERE year=2025 AND month=1 GROUP BY did"
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "SQL query to execute against the posts view",
            }
        },
        "required": ["sql"],
    },
}


def handle_mcp_request(body: dict, actor_proxy) -> dict:
    """Route a JSON-RPC 2.0 MCP request to the actor proxy; never raises."""
    method = body.get("method")
    request_id = body.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "bluesky-query", "version": "1.0.0"},
            },
        }

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"tools": [TOOL_SCHEMA]},
        }

    if method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if tool_name != "query_posts":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32602, "message": f"Unknown tool: {tool_name}"},
            }

        sql = arguments.get("sql")
        if not sql or not sql.strip():
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32602, "message": "Missing required argument: sql"},
            }

        try:
            rows = actor_proxy.query(sql)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(rows)}],
                    "isError": False,
                },
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": str(e)}],
                    "isError": True,
                },
            }

    if method is None:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32600, "message": "Invalid Request: missing 'method'"},
        }
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }