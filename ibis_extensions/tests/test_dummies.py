import ibis
import pandas as pd
from pandas.testing import assert_frame_equal

from ibis_extensions import get_dummies

ibis.set_backend(ibis.duckdb.connect(":memory:", threads=4, memory_limit="1GB"))


def test_get_dummies():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "type": ["A", "A", "B", "B", "C"]})
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "type": ["A", "A", "B", "B", "C"],
            "type_A": [1, 1, 0, 0, 0],
            "type_B": [0, 0, 1, 1, 0],
            "type_C": [0, 0, 0, 0, 1],
        }
    )
    result = get_dummies(table, table.type, drop_input_column=False).execute()
    assert_frame_equal(result, expected, check_dtype=False)


def test_get_dummies_drop_columns():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "type": ["A", "A", "B", "B", "C"]})
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "type_A": [1, 1, 0, 0, 0],
            "type_B": [0, 0, 1, 1, 0],
            "type_C": [0, 0, 0, 0, 1],
        }
    )
    result = get_dummies(table, table.type).execute()
    assert_frame_equal(result, expected, check_dtype=False)


def test_get_dummies_predefined_categories():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "type": ["A", "A", "B", "B", "C"]})
    table = ibis.memtable(df)
    expected = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "type": ["A", "A", "B", "B", "C"],
            "type_A": [1, 1, 0, 0, 0],
            "type_B": [0, 0, 1, 1, 0],
        }
    )
    result = get_dummies(
        table, table.type, ["A", "B"], drop_input_column=False
    ).execute()
    assert_frame_equal(result, expected, check_dtype=False)
