import unittest.mock

import ray
import pytest
import duckdb
from src.actor import DuckDBQueryActor, QueryError, S3Error


@pytest.fixture(scope="module")
def ray_local():
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture(scope="module")
def actor(ray_local, fixture_parquet_dir):
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    return DuckDBQueryActor.remote(parquet_glob=glob)


def test_query_returns_all_rows(actor):
    rows = ray.get(actor.query.remote("SELECT * FROM posts"))
    assert len(rows) == 5


def test_query_returns_dicts_with_correct_keys(actor):
    rows = ray.get(actor.query.remote("SELECT * FROM posts LIMIT 1"))
    assert isinstance(rows[0], dict)
    assert "did" in rows[0]
    assert "record_text" in rows[0]


def test_query_filters_by_did(actor):
    rows = ray.get(
        actor.query.remote("SELECT * FROM posts WHERE did = 'did:plc:abc123'")
    )
    assert len(rows) == 3
    assert all(r["did"] == "did:plc:abc123" for r in rows)


def test_query_aggregation(actor):
    rows = ray.get(
        actor.query.remote(
            "SELECT did, COUNT(*) as cnt FROM posts GROUP BY did ORDER BY cnt DESC"
        )
    )
    assert rows[0]["did"] == "did:plc:abc123"
    assert rows[0]["cnt"] == 3


def test_invalid_sql_raises_query_error(actor):
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(actor.query.remote("NOT VALID SQL AT ALL"))
    assert "QueryError" in str(exc_info.value)


def test_hive_partition_filter(actor):
    rows = ray.get(
        actor.query.remote("SELECT * FROM posts WHERE year = 2025 AND month = 1")
    )
    assert len(rows) == 5


def test_cte_query_is_allowed(actor):
    rows = ray.get(
        actor.query.remote(
            "WITH abc123_posts AS (SELECT * FROM posts WHERE did = 'did:plc:abc123') "
            "SELECT COUNT(*) AS cnt FROM abc123_posts"
        )
    )
    assert rows[0]["cnt"] == 3


def test_cte_wrapping_dml_is_rejected(actor):
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(
            actor.query.remote(
                "WITH cte AS (SELECT 1) DELETE FROM posts WHERE did='did:plc:abc123'"
            )
        )
    assert "QueryError" in str(exc_info.value)


def test_ioexception_raises_s3error(fixture_parquet_dir):
    """duckdb.IOException raised during conn.execute is wrapped as S3Error."""
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    # Instantiate the underlying actor class directly (bypassing Ray).
    ActorClass = DuckDBQueryActor.__ray_actor_class__
    instance = ActorClass.__new__(ActorClass)
    ActorClass.__init__(instance, parquet_glob=glob)

    # Replace the real connection with one whose execute() raises IOException.
    mock_conn = unittest.mock.MagicMock()
    mock_conn.execute.side_effect = duckdb.IOException("S3 auth failed")
    instance.conn = mock_conn

    with pytest.raises(S3Error, match="S3 auth failed"):
        instance.query("SELECT 1")


def test_s3_endpoint_with_single_quote_raises_s3error(fixture_parquet_dir):
    """S3_ENDPOINT containing a single quote must raise S3Error, not cause a SQL syntax error."""
    import os
    from unittest.mock import patch, MagicMock
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    env = {
        "S3_ENDPOINT": "minio.example.com'",
        "S3_ACCESS_KEY": "key",
        "S3_SECRET_KEY": "secret",
    }
    ActorClass = DuckDBQueryActor.__ray_actor_class__
    with patch.dict(os.environ, env):
        with patch("src.actor.duckdb.connect") as mock_duckdb_connect:
            mock_conn = MagicMock()
            mock_duckdb_connect.return_value = mock_conn
            with pytest.raises(S3Error, match="single quote"):
                instance = ActorClass.__new__(ActorClass)
                ActorClass.__init__(instance, parquet_glob=glob)


def test_actor_proxy_reraises_query_error_not_ray_task_error(ray_local, fixture_parquet_dir):
    """ActorProxy must unwrap RayTaskError and re-raise the original typed exception."""
    from src.service import ActorProxy
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    remote = DuckDBQueryActor.remote(parquet_glob=glob)
    proxy = ActorProxy(remote)
    with pytest.raises(QueryError):
        proxy.query("NOT VALID SQL AT ALL")


def test_init_does_not_create_view_eagerly():
    """__init__ must complete without executing CREATE VIEW — deferred to first query."""
    import os
    from unittest.mock import patch, MagicMock

    glob = "s3://bluesky-data/year=*/month=*/day=*/hour=*/*.parquet"
    env = {"S3_ENDPOINT": "minio.example.com", "S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s"}

    ActorClass = DuckDBQueryActor.__ray_actor_class__

    mock_conn = MagicMock()
    executed_sql = []
    mock_conn.execute.side_effect = lambda sql, *a, **kw: executed_sql.append(sql)

    with patch.dict(os.environ, env):
        with patch("src.actor.duckdb.connect", return_value=mock_conn):
            instance = ActorClass.__new__(ActorClass)
            ActorClass.__init__(instance, parquet_glob=glob)

    create_view_calls = [s for s in executed_sql if "CREATE" in s.upper() and "VIEW" in s.upper()]
    assert create_view_calls == [], f"__init__ must not call CREATE VIEW but got: {create_view_calls}"


def test_actor_has_max_concurrency_gt_1():
    """Actor must allow concurrent tasks so ping() can respond while query() runs."""
    options = DuckDBQueryActor._default_options
    assert options.get("max_concurrency", 1) >= 2, (
        "max_concurrency must be >= 2 so health-check pings are not blocked by long-running queries"
    )


def test_httpfs_install_failure_propagates_as_s3error(fixture_parquet_dir):
    """A non-'already-installed' duckdb.Error during INSTALL httpfs must propagate, not be silenced."""
    import os
    from unittest.mock import patch, MagicMock
    glob = str(
        fixture_parquet_dir / "year=*" / "month=*" / "day=*" / "hour=*" / "*.parquet"
    )
    env = {"S3_ENDPOINT": "minio.example.com", "S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s"}

    ActorClass = DuckDBQueryActor.__ray_actor_class__

    def execute_side_effect(sql, *a, **kw):
        if "LOAD" in sql and "httpfs" in sql.lower():
            raise duckdb.Error("httpfs not found")
        if "INSTALL" in sql:
            raise duckdb.IOException("Network unreachable")

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = execute_side_effect

    with patch.dict(os.environ, env):
        with patch("src.actor.duckdb.connect", return_value=mock_conn):
            with pytest.raises(S3Error, match="httpfs"):
                instance = ActorClass.__new__(ActorClass)
                ActorClass.__init__(instance, parquet_glob=glob)