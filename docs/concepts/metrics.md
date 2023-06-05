# Execution and Data Metrics

Flowman provides some important execution metrics and can also collect data quality metrics, which are defined by the
developer. These metrics can then be pushed to a [Prometheus push gateway](../spec/metric/prometheus.md) or stored
in a relation database via a [JDBC sink](../spec/metric/jdbc.md).

These metrics complement [data quality checks](../cookbook/data-quality.md), which have the characteristics of a static 
documentation, while metrics provide (together with appropriate tools like Grafana) a historic view.


## Metric Anatomy
Each metric is made up of a *name*, a map of key values called *labels* and a numerical *value*. The *name* defines
the semantic of a metric, for example the metric called `target_runtime` will contain the runtime of an [execution
target](../spec/target/index.md). But obviously simply having a metric `target_runtime` is not enough, since a typical
Flowman project will contain multiple targets and multiple [execution phases](lifecycle.md) will be 
performed. In order to distinguish all these different measurements of an execution time, *labels* are attached to
each instance of the `target_runtime` metric.

Therefore, each instance of `target_runtime` will have the following labels:
* `namespace` - Name of the Flowman namespace (typically `default`)
* `project` - Name of the Flowman project
* `version` - The version of the project
* `phase` - The execution phase (`VALIDATE`, `CREATE`, `BUILD`, ...)
* `categroy` - Always set to `target` as the category of the entity
* `kind` The kind of the target (`relation`, `copy`, `validate`, ...)
* `name` The name of the target

You will then be able to select specific instances by using these labels.


## Collecting Metrics

Currently, Flowman provides three different ways of *collecting metrics*, which are presented below. 

### Builtin Metrics
First, Flowman already provides a small set of built-in metrics, which are collected automatically.

| Metric           | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `job_runtime`    | Contains the execution runtime of a job, measured in milliseconds           |
| `target_runtime` | Contains the execution runtime of a single target, measured in milliseconds |

Some special targets or relations may provide additional metrics, these are described in the corresponding 
documentation. For example the [relation target](../spec/target/relation.md) also provides a `target_records`
metric.


### Measure Target
The [`measure` target](../spec/target/measure.md) allows you to collect additional data dependent metrics. This is a 
great way to define data quality related metrics. For example the following target `measures` would count the total
number of records and sum up a column `transaction_amount`. It will provide two metrics `transaction_count` and
`total_transaction_amount`:

```yaml
targets:
  measures:
    kind: measure
    measures:
      record_stats:
        kind: sql
        query: "
          SELECT
            COUNT(*) AS transaction_count,
            SUM(transaction_amount) AS total_transaction_amount
          FROM all_transactions"
```



### Measure Mapping
Since version 0.30.0, Flowman also offers a [`observe` mapping](../spec/mapping/observe.md), which allows one to 
observe data as it flows through the system. This is much cheaper in execution than a `measure` target, but it comes
with more limitations of what it can do.

The following example would essentially do the same as the example above. But you won't need to execute a separate
target, you just need to replace all usages of `all_transactions` with `all_transactions_observed` in order to observe
the records as they flow through this mapping.

```yaml
mappings:
  all_transactions_observed:
    kind: observe
    input: all_transactions
    measures:
      transaction_count: "COUNT(*)"
      total_transaction_amount: "SUM(transaction_amount)"
```

## Publishing Metrics
Now that you have collected some metrics, you now want to publish them to a metric collector like Prometheus or a small
SQL database. This is done in two steps: First you need to define a *metric board* in your job, which selects and
possibly renames the metrics to be published. Then you need to provide one or more *metric sinks* where these metrics
will be published to.


### Metric Board
A *metric board* is part of a [job definition](../spec/job/index.md) and is used for selecting the metrics to be 
exported:

```yaml
jobs:
  daily:
    metrics:
      labels:
        force: ${force}
        phase: ${phase}
        status: ${status}
        datetime: ${processing_datetime}
        period: ${processing_duration}
      metrics:
        # This metric contains the number of records per output cube
        - name: flowman_output_records
          selector:
            name: target_records
            # Only select the metric with the following labels:
            labels:
              phase: BUILD
              category: target
          labels:
            cube: ${name}
        # This metric contains the processing time per output
        - name: flowman_output_time
          selector:
            name: target_runtime
            # Only select the metric with the following labels:
            labels:
              phase: BUILD
              category: target
          labels:
            output: ${name}
        # This metric contains the overall processing time
        - name: flowman_processing_time
          selector:
            name: job_runtime
            # Only select the metric with the following labels:
            labels:
              phase: BUILD
              category: job
        - name: transaction_count
          selector:
            name: transaction_count
        - name: total_transaction_amount
          selector:
            name: total_transaction_amount
```
The example will export the following metrics

| Exported Metric            | Flowman Metric             | Restrictions                      |
|----------------------------|----------------------------|-----------------------------------|
| `flowman_output_records`   | `target_records`           | Only for targets in `BUILD` phase |
| `flowman_output_time`      | `target_runtime`           | Only for targets in `BUILD` phase |
| `flowman_processing_time`  | `job_runtime`              | Only for jobs in `BUILD` phase    |
| `transaction_count`        | `transaction_count`        |                                   |
| `total_transaction_amount` | `total_transaction_amount` |                                   |


### Metric Sink

Finally, you need to specify, *where* the collected metrics should be published to. This is a central setting in the
[namespace](../spec/namespace.md):

```yaml
name: "default"

metrics:
  # Dump all metrics onto the console
  - kind: console
  # Publish metrics to a Prometheus push gateway
  - kind: prometheus
    url: $System.getenv('URL_PROMETHEUS_PUSHGW')
    # Attach additional labels for Prometheus
    labels:
      job: flowman
      instance: default
      phase: $phase
```
