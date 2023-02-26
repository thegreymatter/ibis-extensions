import ibis
import pandas as pd
from pandas.testing import assert_frame_equal

from ibis_extensions import pivot

ibis.set_backend(ibis.duckdb.connect(":memory:", threads=4, memory_limit="1GB"))


def test_pivot_sum():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 2, 2],
            "type": ["A", "A", "B", "B", "C"],
            "value": [7, 3, 5, 14, 3],
        }
    )
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [
                1,
                2,
            ],
            "type_A": [10, None],
            "type_B": [5, 14],
            "type_C": [None, 3],
        }
    )
    result = pivot(table, table.id, table.type, table.value).execute()
    assert_frame_equal(result, expected, check_dtype=False)


def test_pivot_mean():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 2, 2],
            "type": ["A", "A", "B", "B", "C"],
            "value": [7, 3, 5, 14, 3],
        }
    )
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [
                1,
                2,
            ],
            "type_A": [5, None],
            "type_B": [5, 14],
            "type_C": [None, 3],
        }
    )
    result = pivot(
        table, table.id, table.type, table.value, aggregation_function="mean"
    ).execute()
    assert_frame_equal(result, expected, check_dtype=False)
