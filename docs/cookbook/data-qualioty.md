# Data Quality

Data quality is an important topic, which is also addressed in Flowman. The special [measure target](../spec/target/measure.md)
provides some means to collect some important metrics from data and provide the results as metrics. These in turn
can be [published to Prometheus](metrics.md) or other metric collectors.


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

This example will publish two metrics, `record_count` and `column_sum`, which then can be sent to a
[metric sink](../spec/metric) configured in the [namespace](../spec/namespace.md).
