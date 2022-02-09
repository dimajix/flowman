# Validate Target

The `validate` target is used to execute a set of assertions in advance of the `CREATE` and `BUILD` phases. This is a
good place to validate any assumptions on the input data like primary key or record count.


## Example

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

## Fields

* `kind` **(mandatory)** *(type: string)*: `validate`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `assertions` **(optional)** *(type: map:assertion)*:
  Map of [assertions](../assertion/index.md) to be executed. The validation is marked as *failed* if a single
  assertion fails.

* `mode`  **(mandatory)** *(type: string)* *(default: `failFast`)*:
  Specify how to proceed in case individual assertions fail. Possible values are `failFast`, `failAtEnd` and `failNever`


## Supported Phases
* `VALIDATE` - The specified assertions will be run in the `VALIDATE` phase before the `CREATE` and `BUILD` phases.


## Remarks

This build target works very similar to the [`verify`](verify.md) target, except that it is only active during the
`VALIDATE` phase and the default for the `mode` parameter is set to `failFast`. Here the assumption is that a validation
should check preconditions before a project is executed. If one of these preconditions fail, there is no value in
executing the rest of the project. So you want to fail as early as possible to save execution time.
