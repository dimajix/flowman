# Data Quality

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


## Data Quality Tests as Documentation

With the new [documentation framework](../documenting/index.md), Flowman adds the possibility not only to document
mappings and relations, but also to add test cases. These will be executed as part of the documentation (which is
generated with an independent command with [`flowexec`](../cli/flowexec.md)).


## Data Quality Metrics
In addition to the `validate` and `verify` targets, Flowman also offers a special [measure target](../spec/target/measure.md).
This target provides some means to collect some important metrics from data and provide the results as metrics. These 
in turn can be [published to Prometheus](metrics.md) or other metric collectors.


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
[metric sink](../spec/metric) configured in the [namespace](../spec/namespace.md).
