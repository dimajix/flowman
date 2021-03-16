# Validate Target

The `validate` target is used to execute a set of assertions in advance of the `CREATE` and `BUILD` phases. This is a
good place to validate any assumptions on the input data like primary key or record count.


## Example

```yaml
targets:
  validate_input:
    kind: validate
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

* `assertions` **(optional)** *(type: map:assertion)*:
  Map of [assertions](../assertion/index.md) to be executed. The validation is marked as *failed* if a single
  assertion fails.


## Supported Phases
* `VALIDATE` - The specified assertions will be run in the `VALIDATE` phase before the `CREATE` and `BUILD` phases.
