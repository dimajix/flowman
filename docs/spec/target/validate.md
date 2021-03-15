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
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `validate`

* `assertions` **(optional)** *(type: map:assertion)*:
  List of assertions to be executed


## Supported Phases
* `VALIDATE` - The specified assertions will be run in the `VALIDATE` phase before the `CREATE` and `BUILD` phases.
