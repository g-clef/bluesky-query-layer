import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture(scope="session")
def fixture_parquet_dir(tmp_path_factory):
    base = tmp_path_factory.mktemp("parquet")
    partition = base / "year=2025" / "month=1" / "day=15" / "hour=10"
    partition.mkdir(parents=True)

    table = pa.table(
        {
            "collection_timestamp": pa.array(
                [datetime.datetime(2025, 1, 15, 10, i, 0) for i in range(5)]
            ),
            "event_timestamp": pa.array(
                [datetime.datetime(2025, 1, 15, 10, i, 0) for i in range(5)]
            ),
            "did": pa.array(
                [
                    "did:plc:abc123",
                    "did:plc:def456",
                    "did:plc:abc123",
                    "did:plc:ghi789",
                    "did:plc:abc123",
                ]
            ),
            "event_type": pa.array(["create"] * 5),
            "collection": pa.array(["app.bsky.feed.post"] * 5),
            "action": pa.array(["create"] * 5),
            "record_text": pa.array(
                ["Hello world", "Test post", "Another post", "Hi there", "Final post"]
            ),
            "reply_parent": pa.array(
                [None, None, None, None, None], type=pa.string()
            ),
            "reply_root": pa.array([None, None, None, None, None], type=pa.string()),
            "embed_type": pa.array([None, None, None, None, None], type=pa.string()),
            "raw_event": pa.array(['{"type": "post"}'] * 5),
        }
    )
    pq.write_table(table, partition / "data.parquet")
    return base