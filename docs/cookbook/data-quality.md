# Data Quality Checks

Data quality is an important topic, which is also addressed in Flowman in multiple, complementary ways. 


## Verification and Validation

First you might want to add some [validate](../spec/target/validate.md) and [verify](../spec/target/verify.md) targets
to your job. The `validate` the target will be executed before the `CREATE` phase and is well suited for performing some tests
on the source data. If these tests fail, you may either emit a simple warning or stop the build altogether in failed
state (which is the default behaviour).

The `verify` target will be executed in the `VERIFY` phase after the `BUILD` phase and is well suited for conducting
data quality tests after the build itself has finished. Again a failing `verify` target may either only generate a
warning, or may fail the build.

### Example

```yaml
targets:
  validate_input:
    kind: validate
    mode: failFast
    assertions:
      assert_primary_key:
        kind: sql
        tests:
          - query: "SELECT id,count(*) FROM source GROUP BY id HAVING count(*) > 0"
            expected: []

      assert_measurement_count:
        kind: sql
        tests:
          - query: "SELECT COUNT(*) FROM measurements_extracted"
            expected: 2
```


## Data Quality Checks as Documentation

With the new [documentation framework](../documenting/index.md), Flowman adds the possibility not only to document
mappings and relations, but also to add test cases. These will be executed as part of the documentation (which is
generated with an independent command with [`flowexec`](../cli/flowexec/index.md)).


## Data Quality Metrics
In addition to the `validate` and `verify` targets, Flowman also offers a special [measure target](../spec/target/measure.md).
This target provides some means to collect some important metrics from data and provide the results as metrics. These 
in turn can be [published to Prometheus](execution-metrics.md) or other metric collectors.


### Example

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
[metric sink](../spec/metric/index.md) configured in the [namespace](../spec/namespace.md).


## When to use what
All three approaches are complementary and can be used together. It all depends on what you want to achieve.

### Checking Pre- and Post-Conditions
If you want to verify that certain pre- or post-conditions in the source or output data are met, then the
[`validate`](../spec/target/validate.md) and [`verify`](../spec/target/verify.md) targets should be used. They
will perform arbitrary tests either before the `CREATE` and `BUILD` phase (in case of the `validate` target) or after
the `BUILD` phase (in case of the `verify` target). In case any of the tests fail, the whole build will fail and not
proceed any processing. This approach can be used to only start the data transformations when input data is clean and
matches your expectations.

### Continuous Monitoring of Data Quality
If you want to setup some continuous monitoring of your data quality (either input or output or both), then the
[`measure` target](../spec/target/measure.md) is the right choice. It will collect arbitrary numerical metrics from
the data and publish it to a metrics sink like Prometheus. Typically, metric collectors are used in conjunction with
a dashboard (like Grafana), which then can be used to display the whole history of these metrics over time. This way
you can see if data quality improves or gets worse, and many of these tools also allow you to set up alarms when
some threshold is reached.

### Documenting Expectations with Reality Check
Finally, the whole documentation subsystem is the right tool for specifying your expectations on the data quality and
have these expectations automatically checked with the real data. In combination with continuous monitoring this can
help to better understand what might be going wrong. In contrast to pre/post-condition checking, a failed check in
the documentation will not fail the build - it will simply be marked as failed in the documentation, but that's all
what will happen.
