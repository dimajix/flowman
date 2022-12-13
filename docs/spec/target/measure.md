# Measure Target

The `measure` target will perform some measurements which then are provided as execution metrics. These measurements
are used to assess data quality. The measures to be taken are specified as [measure](../measure/index.md) instances.

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
[metric sink](../metric/index.md) configured in the [namespace](../namespace.md).


## Provided Metrics
All metrics defined as named columns are exported with the following labels:
    - `name` - The name of the measure (i.e. `record_stats` above)
    - `category` - Always set to `measure`
    - `kind` - Always set to `sql`
    - `namespace` - Name of the namespace (typically `default`)
    - `project` - Name of the project
    - `version` - Version of the project


## Supported Execution Phases
* `VERIFY` - The evaluation of all measures will only be performed in the `VERIFY` phase

Read more about [execution phases](../../concepts/lifecycle.md).


## Dirty Condition
A `measure` target is always dirty for the `VERIFY` execution phase.
