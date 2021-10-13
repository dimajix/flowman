# Execution Metrics

Flowman also provides some execution metrics in addition to those already provided by Spark. Flowman metrics include
execution times for individual build targets, number of records written etc. The documentation always contains an
information whenever some metrics are collected.

In order to actually use these metrics, you need to configure two things:
* A *metrics sink* (like [Prometheus](../spec/metric/prometheus.md))
* A *metrics board* containing the configuration of which metrics should be published

## Example
The mapping of how metrics should be exported is part of the job definition as follows:
```yaml
jobs:
  daily:
    description: "Process whole range of periods"
    parameters:
      - name: processing_datetime
        type: timestamp
        description: "Specifies the date in yyyy-MM-dd for which the result will be generated"
    targets:
      - my_target
      - my_other_target
    metrics:
      labels:
        force: ${force}
        status: ${status}
        phase: ${phase}
        datetime: ${processing_datetime}
      metrics:
        # This metric contains the number of records per output. It will search all metrics called
        # `target_records` and export them as `flowman_output_records`. It will also label each metric with
        # the name of each Flowman build target (in case you have multiple targets)
        - name: flowman_output_records
          selector:
            name: target_records
            labels:
              category: target
          labels:
            cube: ${name}
        # This metric contains the processing time per output. Again the selector will search for all metrics
        # named `target_runtime` provided by a `target` category and will export these metrics as
        # `flowman_output_time` with a label called `output` containing the name of the Flowman build target
        - name: flowman_output_time
          selector:
            name: target_runtime
            labels:
              category: target
          labels:
            output: ${name}
        # This metric contains the overall processing time
        - name: flowman_processing_time
          selector:
            name: job_runtime
            labels:
              category: job
```

Now you only need to provide a metric sink in the `default-namespace.yml` file
```yaml
metrics:
  kind: prometheus
  url: $System.getenv('URL_PROMETHEUS_PUSHGW')
  labels:
    job: "daily"
    instance: "default"
    namespace: $System.getenv('NAMESPACE')
```
