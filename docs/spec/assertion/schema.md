# Schema Assertion

The `schema` assertion verifies that the output of a mapping conforms to a specific schema. This may be useful at the
end of complex transformation chains to ensure a given data interface contract is met.

## Example:

```yaml
kind: schema
mapping: some_mapping
ignoreTypes: true
columns:
  col_1: string
  col_2: int
```

```yaml
kind: schema
mapping: some_mapping
ignoreNullability: false
schema:
  kind: inline
  fields:
    - name: col_1
      type: string
    - name: col_2
      type: int
```

A more complete example using the [`validate` target](../target/validate.md) might look as follows:
```yaml
targets:
  validate_export_schema:
    kind: validate
    assertions:
      - kind: schema
        mapping: some_mapping
        ignoreNullability: true
        ignoreCase: false
        ignoreOrder: true
        schema:
          kind: avro
          file: ${project.basedir}/schema/export-schema.avsc
          
  perform_export:
    kind: relation
    relation: export_relation
    mapping: some_mapping

jobs:
  export:
    targets:
      - validate_export_schema
      - perform_export
```
This example will validate that the result of the mapping `some_mapping` actually conforms to a schema in some Avro
schema definition. Since the validation is performed in the `VALIDATE` phase, even before the `CREATE` and `BUILD` phase,
the job will fail if the schema doesn't match and the export as defined in the target `perform_export` won't be 
performed.

Of course, you can also use the `schema` assertion within the [Flowman testing framework](../../testing/index.md),
such that the test will not be performed during the real execution of an export job, but as part of an integration
test, which is typically conducted only as part of a continuous integration (CI) process.


## Fields

* `kind` **(mandatory)** *(type: string)*: `schema`

* `description` **(optional)** *(type: string)*:
  A textual description of the assertion

* `mapping` **(required)** *(type: string)*:
  The name of the mapping which is to be tested.

* `columns` **(optional)** *(type: map:string)*:
  Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
  As an alternative of specifying a list of columns you can also directly specify a schema.

* `ignoreTypes` **(optional)** *(type: boolean)* *(default: false)*:
  Ignore types, only field names will be compared.

* `ignoreNullability` **(optional)** *(type: boolean)* *(default: false)*:
  Ignore nullability of fields, only field names and types will be compared.

* `ignoreCase` **(optional)** *(type: boolean)* *(default: false)*:
  Ignore upper/lower case of field names.

* `ignoreOrder` **(optional)** *(type: boolean)* *(default: false)*:
  Ignore the order of fields. The schema matches the given schema as long as all fields are present. Otherwise, the
  order of all fields also has to match (which is the default) 
