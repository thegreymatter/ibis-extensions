from datetime import date

import ibis
import pandas as pd
from pandas._testing import assert_frame_equal

from ibis_extensions import merge_timeframes
from ibis_extensions.time_series import cut_timeframes

ibis.set_backend(ibis.duckdb.connect(":memory:", threads=4, memory_limit="1GB"))


def test_merge_timeframes_inconsistent_timeframes():
    df = pd.DataFrame(
        {
            "id": [1, 1, 1, 1],
            "start": [
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
            ],
            "end": [
                date(2020, 2, 1),
                date(2020, 2, 1),
                date(2020, 2, 1),
                date(2020, 2, 1),
            ],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [1],
            "start": [
                date(2020, 1, 1),
            ],
            "end": [
                date(2020, 2, 1),
            ],
        }
    )

    table = ibis.memtable(df)
    result = merge_timeframes(table, table.start, table.end, keys=[table.id]).execute()

    assert_frame_equal(result, expected, check_dtype=False)


def test_merge_timeframes_overlapping_timeframes():
    df = pd.DataFrame(
        {
            "id": [1, 1],
            "start": [date(2020, 1, 1), date(2020, 2, 1)],
            "end": [date(2020, 2, 1), date(2020, 3, 1)],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [1],
            "start": [
                date(2020, 1, 1),
            ],
            "end": [
                date(2020, 3, 1),
            ],
        }
    )

    table = ibis.memtable(df)
    result = merge_timeframes(table, table.start, table.end, keys=[table.id]).execute()

    assert_frame_equal(result, expected, check_dtype=False)


def test_merge_timeframes_non_overlapping_timeframes():
    df = pd.DataFrame(
        {
            "id": [1, 1],
            "start": [date(2020, 1, 1), date(2020, 3, 1)],
            "end": [date(2020, 2, 1), date(2020, 4, 1)],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [1, 1],
            "start": [date(2020, 1, 1), date(2020, 3, 1)],
            "end": [date(2020, 2, 1), date(2020, 4, 1)],
        }
    )

    table = ibis.memtable(df)
    result = merge_timeframes(table, table.start, table.end, keys=[table.id]).execute()

    assert_frame_equal(result, expected, check_dtype=False)


def test_merge_timeframes_spillted_keys():
    df = pd.DataFrame(
        {
            "id": [1, 2, 2, 3],
            "start": [
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
            ],
            "end": [
                date(2020, 2, 1),
                date(2020, 2, 1),
                date(2020, 3, 1),
                date(2020, 3, 1),
            ],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "start": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 1)],
            "end": [date(2020, 2, 1), date(2020, 3, 1), date(2020, 3, 1)],
        }
    )

    table = ibis.memtable(df)
    result = merge_timeframes(table, table.start, table.end, keys=[table.id]).execute()

    assert_frame_equal(result, expected, check_dtype=False)


def test_cut_timeframes_limit_literal():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "start": [
                date(2019, 1, 1),
                date(2020, 1, 1),
                date(2021, 1, 1),
            ],
            "end": [
                date(2020, 2, 1),
                date(2021, 2, 1),
                date(2022, 3, 1),
            ],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [2],
            "start": [
                date(2020, 4, 1),
            ],
            "end": [
                date(2020, 8, 1),
            ],
        }
    )

    table = ibis.memtable(df)
    result = cut_timeframes(
        table, table.start, table.end, date(2020, 4, 1), date(2020, 8, 1)
    ).execute()

    assert_frame_equal(result, expected, check_dtype=False)


def test_cut_timeframes_limit_column():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "start": [
                date(2019, 1, 1),
                date(2020, 1, 1),
                date(2021, 1, 1),
            ],
            "end": [
                date(2020, 2, 1),
                date(2021, 2, 1),
                date(2022, 3, 1),
            ],
        }
    )

    expected = pd.DataFrame(
        {
            "id": [2],
            "start": [
                date(2020, 4, 1),
            ],
            "end": [
                date(2020, 8, 1),
            ],
            "start_limit": [
                date(2020, 4, 1),
            ],
            "end_limit": [
                date(2020, 8, 1),
            ],
        }
    )

    table = ibis.memtable(df).mutate(
        start_limit=date(2020, 4, 1), end_limit=date(2020, 8, 1)
    )
    result = cut_timeframes(
        table, table.start, table.end, table.start_limit, table.end_limit
    ).execute()

    assert_frame_equal(result, expected, check_dtype=False)
