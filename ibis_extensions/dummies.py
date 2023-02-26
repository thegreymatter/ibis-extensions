from typing import Optional
import ibis
from ibis import Expr
from ibis.expr.types import Table, Column


def _get_categories(df: Table, cat_col: Column):
    return df.select(cat_col).distinct().execute().iloc[:, 0].tolist()


class OneHotEncoder:
    def __init__(
        self,
        input_col: Optional[str] = None,
        categories: Optional[list[str]] = None,
        drop_input_column=True,
    ):
        self._fitted_values = []
        self.input_col: Column = input_col
        self.categories = categories
        self.drop_input_column = drop_input_column

    def fit(self, df: Expr) -> "OneHotEncoder":
        if not self.categories:
            self._fitted_values = _get_categories(df, self.input_col)
        else:
            self._fitted_values = self.categories

        return self

    def transform(self, df: Expr) -> Expr:
        terms = {}
        for category in self._fitted_values:
            terms[f"{self.input_col.get_name()}_{category}"] = (
                ibis.case()
                .when(df[self.input_col.get_name()] == category, 1)
                .else_(0)
                .end()
            )
        if self.drop_input_column:
            df = df.drop(self.input_col.get_name())
        return df.mutate(**terms)

    def fit_transform(self, df: Expr) -> Expr:
        return self.fit(df).transform(df)


def get_dummies(
    table: Table,
    input_col: Column,
    categories: list[any] = None,
    drop_input_column=True,
) -> Expr:
    """Convert categorical variables into dummy/indicator variables.

    Parameters
    ----------
    table
        table expression.
    input_col

    categories
        order to use when choosing how to drop duplicates.

    Returns
    -------
    Table
        Table expression with unique rows
    """
    ohc = OneHotEncoder(input_col, categories, drop_input_column)
    return ohc.fit_transform(table)
