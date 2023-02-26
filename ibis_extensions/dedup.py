from ibis import _
import ibis
from ibis.expr.types import Table, Column, Value


def dedup(
    table: Table, keys: [list[Column], None] = None, order_by: Value = None
) -> Table:
    """Drop duplicate rows from this table.

    Parameters
    ----------
    table
        table expression.
    keys
        Columns to use when detecting duplicates. When set to None, use all columns.
    order_by
        order to use when choosing how to drop duplicates.

    Returns
    -------
    Table
        Table expression with unique rows
    """
    if not keys:
        keys = table.get_columns(table.columns)
    return (
        table.mutate(
            keep_row=ibis.row_number().over(
                ibis.window(group_by=keys, order_by=order_by)
            )
        )
        .filter(_.keep_row == 0)
        .drop("keep_row")
    )
