# Verify Target

The `verify` target is used to execute a set of assertions *after* all targets have been built. This can be used to
verify the results to ensure that all processing was correct. In most cases it is advisable to use the similar 
[`validate`](validate.md) build target, which is executed in advance of the `CREATE` and `BUILD` phase and can be
used to validate any assumptions on the incoming data.

## Example

```yaml
targets:
  verify_output:
    kind: verify
    mode: failNever
    assertions:
      assert_primary_key:
        kind: sql
        tests:
          - query: "SELECT id,count(*) FROM output_table GROUP BY id HAVING count(*) > 0"
            expected: []
          - query: "SELECT id,count(*) FROM output_cube GROUP BY id HAVING count(*) > 0"
            expected: []
      
      assert_measurement_count:
        kind: sql
        query: "SELECT COUNT(*) FROM measurements_extracted"
        expected: 2
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `verify`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `assertions` **(optional)** *(type: map:assertion)*:
  Map of [assertions](../assertion/index.md) to be executed. The verification is marked as *failed* if a single
  assertion fails.

* `mode`  **(mandatory)** *(type: string)* *(default: `failAtEnd`)*:
  Specify how to proceed in case individual assertions fail. Possible values are `failFast`, `failAtEnd` and `failNever`


## Supported Execution Phases
* `VERIDY` - The specified assertions will be run in the `VERIFY` phase after the `CREATE` and `BUILD` phases.

Read more about [execution phases](../../lifecycle.md).


## Remarks

This build target works very similar to the [`validate`](validate.md) target, except that it is only active during the
`VERIFY` phase and the default for the `mode` parameter is set to `failAtEnd`. Here the assumption is that a verification
should check postconditions implied by the logic of your Flowman project. Flowman should return an error in this case,
but since the fault is on your side, you want to see all failed checks and not only the first one.
