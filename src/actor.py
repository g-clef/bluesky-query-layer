import os

import duckdb
import ray

from src.exceptions import QueryError, QueryTimeoutError, S3Error


@ray.remote
class DuckDBQueryActor:
    def __init__(self, parquet_glob: str | None = None) -> None:
        self.conn = duckdb.connect()

        s3_endpoint = os.environ.get("S3_ENDPOINT", "")
        # DuckDB s3_endpoint expects host[:port] only, no scheme.
        for scheme in ("https://", "http://"):
            if s3_endpoint.startswith(scheme):
                s3_endpoint = s3_endpoint[len(scheme):]
                break
        if s3_endpoint:
            if "'" in s3_endpoint:
                raise S3Error("S3_ENDPOINT must not contain single quotes")
            try:
                self.conn.execute("INSTALL httpfs;")
            except duckdb.Error as e:
                if "already" not in str(e).lower():
                    raise S3Error(f"Failed to install httpfs extension: {e}") from e
            self.conn.execute("LOAD httpfs;")
            self.conn.execute(f"SET s3_endpoint='{s3_endpoint}';")

            s3_access_key = os.environ.get("S3_ACCESS_KEY")
            if s3_access_key is None:
                raise S3Error("Missing required env var: S3_ACCESS_KEY")
            if "'" in s3_access_key:
                raise S3Error("S3_ACCESS_KEY must not contain single quotes")

            s3_secret_key = os.environ.get("S3_SECRET_KEY")
            if s3_secret_key is None:
                raise S3Error("Missing required env var: S3_SECRET_KEY")
            if "'" in s3_secret_key:
                raise S3Error("S3_SECRET_KEY must not contain single quotes")

            self.conn.execute(f"SET s3_access_key_id='{s3_access_key}';")
            self.conn.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
            self.conn.execute("SET s3_use_ssl=false;")
            self.conn.execute("SET s3_url_style='path';")

        glob = parquet_glob or os.environ.get(
            "PARQUET_GLOB",
            "s3://bluesky-data/year=*/month=*/day=*/hour=*/*.parquet",
        )
        if "'" in glob:
            raise ValueError(
                "parquet_glob must not contain single quotes"
            )
        self.conn.execute(
            f"CREATE OR REPLACE VIEW posts AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )

    def query(self, sql: str) -> list[dict]:
        stripped = sql.lstrip().lower()
        if not (stripped.startswith("select") or stripped.startswith("with")):
            raise QueryError("Only SELECT queries are allowed")
        if stripped.startswith("with") and any(
            w in ("update", "delete", "insert") for w in stripped.split()
        ):
            raise QueryError("Only SELECT queries are allowed")
        try:
            result = self.conn.execute(sql)
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]
        # Note: some S3 auth failures may surface as duckdb.Error instead of
        # duckdb.IOException and will be caught by the broader handler as QueryError.
        except duckdb.IOException as e:
            raise S3Error(str(e)) from e
        except duckdb.Error as e:
            raise QueryError(str(e)) from e

    def ping(self) -> bool:
        return True