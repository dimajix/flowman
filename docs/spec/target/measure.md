# Measure Target

The `measure` target will perform some measurements which then are provided as execution metrics. These measurements
are used to assess data quality. The measures to be taken are specified as [measure](../measure) instances.

## Example

```yaml
targets:
  measures:
    kind: measure
    measures:
      record_stats:
        kind: sql
        query: "
          SELECT
            COUNT(*) AS record_count 
            SUM(column IS NULL)  AS column_sum
          FROM some_mapping"
```

This example will provide two metrics, `record_count` and `column_sum`, which then can be sent to a 
[metric sink](../metric) configured in the [namespace](../namespace.md).
