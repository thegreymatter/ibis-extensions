import ibis
import pandas as pd
from pandas.testing import assert_frame_equal

from ibis_extensions import dedup

ibis.set_backend(ibis.duckdb.connect(":memory:", threads=4, memory_limit="1GB"))


def test_dedup():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 2, 2],
            "type": ["A", "A", "B", "B", "C"],
        }
    )
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 1, 2, 2],
            "type": ["A", "B", "B", "C"],
        }
    )
    result = dedup(table).execute()
    assert_frame_equal(result, expected, check_dtype=False)


def test_dedup_on_single_column():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 2, 2],
            "type": ["A", "A", "B", "B", "C"],
        }
    )
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "type": ["A", "B", "C"],
        }
    )
    result = dedup(table, keys=[table.type]).execute()
    assert_frame_equal(result, expected, check_dtype=False)


def test_dedup_type_col_desc_id():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 2, 2],
            "type": ["A", "A", "B", "B", "C"],
        }
    )
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 2, 2],
            "type": ["A", "B", "C"],
        }
    )
    result = dedup(table, keys=[table.type], order_by=table.id.desc()).execute()
    assert_frame_equal(result, expected, check_dtype=False)
