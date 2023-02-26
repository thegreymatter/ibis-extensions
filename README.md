# ibis-extensions
ibis-extensions is a utility library that helps implementing higher-level code snippets for ibis. 


## Installation
ibis-extensions is published to the Python Package Index (PyPI) under the name snowbear. To install it, run:

``` shell
pip install ibis-extensions
```

## implemented extensions 

### pivot
Pivots table based on column values
```python
result = table.pivotpivot(table, table.id, table.type, table.value, aggregation_function="mean")
```

### get_dummies
Convert categorical variables into indicator variables.

```python
get_dummies(table, table.category)
```

### dedup
Drop duplicate rows from table.

```python
dedup(table, keys=[table.id])
```
to dedup by all columns, call dedup without keys
```python
dedup(table)
```

### merge_timeframes
merge overlapping timeframes
```python
merge_timeframes(table, table.start, table.end, keys=[table.id])
```

### cut_timeframes
cut and filter timeframes in a provided range so not timeframe will be after end_limit or before start_limit. 

```python
cut_timeframes(table, table.start, table.end, start_limit=date(2020, 4, 1), end_limit=date(2020, 8, 1))
```
you may provide a seperate range for each row by providing limit columns
cut_timeframes(table, table.start, table.end, table.start_limit, table.end_limit)
