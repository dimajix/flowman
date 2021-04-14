# Expression Assertion

An assertion of type `expression` allows you to verify specific properties for each record of a specific mapping.
The assertion works like a SQL filter and checks that every record fulfills the given SQL expression.

## Example:

```yaml
kind: expression
description: "Assert that only allowed values are used"
mapping: measurements_extracted
expected:
  - "network IS NOT NULL"
  - "campaign IS IN (1,2)" 
```

A more complete example (with the required top level entities) could look as follows:
```yaml
targets:
  verify_output:
    kind: verify
    assertions:
      assert_facts_columns:
        kind: expression
        description: "Assert that only allowed values are used"
        mapping: measurements_extracted
        expected:
          - "network IS NOT NULL"
          - "campaign IS IN (1,2)" 
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `expression`

* `description` **(optional)** *(type: string)*:
  A textual description of the assertion

* `mapping` **(optional)** *(type: srting)*:
 The name of the mapping which is to be tested.

* `expected` **(optional)** *(type: list:srting)*:
  A list of Spark SQL expressions, which are tested for every single record that is produced by the specified 
  mapping. You can imagine that the expression will be used in a SQL `WHERE` condition, which means that you
  can access all columns from the mapping, but you cannot perform any aggregations or more complex transformations.
  
