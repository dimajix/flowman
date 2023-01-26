# Measure Target

The `measure` target will perform some measurements which then are provided as execution metrics. These measurements
are used to assess data quality. The measures to be taken are specified as [measure](../measure/index.md) instances.

Since a measure target needs to be explicitly executed, it will increase your overall job execution time. Since
version 0.30.0, Flowman offers an alternative [`observe` mapping](../mapping/observe.md), which offers similar (but
more limited) capabilities and is much cheaper from an execution time point of view.

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
            COUNT(*) AS record_count,
            MIN(air_temperature) AS min_temperature,
            MAX(air_temperature) AS max_temperature
          FROM measurements"
```
These metrics then can be published in a job as follows:
```yaml
jobs:
  main:
    targets:
      - measures
    metrics:
      # Add some common labels to all metrics
      labels:
        force: ${force}
        phase: ${phase}
        status: ${status}
      metrics:
        # This metric contains the processing time per output
        - name: flowman_output_time
          selector:
            name: target_runtime
            labels:
              phase: BUILD
              category: target
          labels:
            output: ${name}
        # This metric contains the overall processing time
        - name: flowman_processing_time
          selector:
            name: job_runtime
            labels:
              phase: BUILD
              category: job
        # The following metrics have been defined in the "measures" target
        - name: record_count
          selector:
            name: record_count
        - name: min_temperature
          selector:
            name: min_temperature
        - name: max_temperature
          selector:
            name: max_temperature
```

This example will provide three metrics, `record_count`, `min_temperature` and `max_temperature`, which then can be 
sent to a [metric sink](../metric/index.md) configured in the [namespace](../namespace.md).


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
