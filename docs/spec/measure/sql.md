# SQL Measure

## Example

```yaml
targets:
  my_measures:
    kind: measure
    measures:
      nulls:
        kind: sql
        query: "
          SELECT 
            SUM(col IS NULL) AS col_nulls
          FROM some_mapping
        "
```
