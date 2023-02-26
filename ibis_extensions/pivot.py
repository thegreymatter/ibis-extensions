from typing import Sequence

import ibis
from ibis.expr import datatypes
from ibis.expr.types import Table, Column
from ibis import _


def _get_categories(df: Table, cat_col: Column) -> Sequence[str]:
    return df.select(cat_col).distinct().execute().iloc[:, 0].tolist()


def pivot(
    table: Table,
    key_column: Column,
    pivot_column: Column,
    value_column: Column,
    aggregation_function: str = "sum",
    separator: str = "_",
) -> Table:
    """
    Pivots table based on column values

    Parameters
    ----------
    table : Input table expression.
    key_column : Column to use as pivot key.
    pivot_column : Column to use to make pivot's columns.
    value_column : Column to use for populating new pivot’s values.
    aggregation_function : Aggregate function str or an expression.
    separator : Separator for pivot column names.

    Returns
    -------
    Reshaped table
    """
    categories = _get_categories(table, pivot_column)
    terms = {}
    for category in categories:
        terms[f"{pivot_column.get_name()}{separator}{category}"] = getattr(
            ibis.case()
            .when(
                pivot_column == ibis.literal(category, type=datatypes.str), value_column
            )
            .end(),
            aggregation_function,
        )()
    return table.group_by(key_column).aggregate(**terms)
