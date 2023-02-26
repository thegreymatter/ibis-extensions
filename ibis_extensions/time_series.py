import datetime

import ibis
from ibis.expr.types import Table, Column
from ibis import _

def merge_timeframes(
    table: Table, start: Column, end: Column, keys: list[Column]
) -> Table:
    grouping = table.mutate(
        row_number=ibis.row_number().over(ibis.window(order_by=keys + [start, end])),
        previous_end_date=end.max().over(
            ibis.window(group_by=keys, order_by=[start, end], preceding=(None, 1))
        ),
    )

    islands = grouping.mutate(
        island_start_ind=ibis.case()
        .when(grouping.previous_end_date >= grouping[start.get_name()], 0)
        .else_(1)
        .end(),
        island_id=(
            ibis.case().when(grouping.previous_end_date >= grouping[start.get_name()], 0).else_(1).end()
        )
        .sum()
        .over(ibis.cumulative_window(order_by=grouping.row_number)),
    )

    return (
        islands.groupby(keys + [islands.island_id])
        .aggregate([islands[start.get_name()].min().name(start.get_name()), islands[end.get_name()].max().name(end.get_name())])
        .order_by(keys)
        .drop("island_id")
    )


def cut_timeframes(
    table: Table,
    start: Column,
    end: Column,
    limit_left: [datetime, Column],
    limit_right: [datetime, Column],
) -> Table:
    return table.filter((start < limit_right) & (end > limit_left)).mutate(
        [
            ibis.greatest(_.start, limit_left).name(start.get_name()),
            ibis.least(_.end, limit_right).name(end.get_name())
        ]
    )
