# Validations

In many cases, you'd like to perform some validation of input data before you start processing. For example when
joining data, you often assume some uniqueness constraint on the join key in some tables or mappings. If that
constraint was violated, your processing would probably create multiple entries per key as a result of the join 
operation, which might be very undesirable.

To detect such cases and prevent Flowman from running the processing (which would probably create wrong results
due to the wrong assumptions on uniqueness of some columns), Flowman provides the ability to run some *validations*
before the processing itself starts. Validations can be specified using the 
[`validate` targets](../spec/target/validate.md), which build on the core functionality of the testing framework,
namely [assertions](../spec/assertion/index.md).


## Example
```yaml
mappings:
  some_table:
    kind: readRelation
    relation: some_table
    
targets:
  validate_some_table:
    kind: validate
    assertions:
      check_unique_campaign:
        kind: sql
        description: "There should be at most one entry per campaign"
        query: "SELECT campaign,COUNT() FROM some_table GROUP BY campaign HAVING COUNT() > 1"
        expected: []
      check_not_null:
        kind: sql
        description: "There should be no NULL values in the campaign column"
        query: "SELECT COUNT(*) FROM some_table WHERE campaign IS NULL"
        expected: [0]

jobs:
  main:
    targets:
      - validate_some_table
      - some_output_target
```
The example above will validate assumptions on `some_table` mapping, which reads from some relation also called
`some_table` (for example tha could be a Hive table or a JDBC table). The first assertion validates that the column
`campaign` only contains unique values while the second assertion validates that the column doesn't contain any
`NULL` values.

All `validate` targets are executed during the [VALIDATE](../lifecycle.md) phase, which is executed before any other
build phase. If one of these targets fail, Flowman will stop execution on return an error. This helps to prevent
building invalid data.
