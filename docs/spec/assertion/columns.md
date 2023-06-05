# Columns Assertion

An assertion of type `columns` allows you to verify schema properties, like specific types of columns or presence/absence
of individual columns.

## Example:

```yaml
kind: columns
description: "Verify correctness of column names and types"
mapping: facts_all
expected:
 - "network IS PRESENT"
 - "xyz IS ABSENT"
 - "campaign IS OF TYPE (int,BIGINT)"
 - "lineitem IS OF TYPE float"
```

A more complete example (with the required top level entities) could look as follows:
```yaml
targets:
  verify_output:
    kind: verify
    assertions:
      assert_facts_columns:
        kind: columns
        description: "Verify correctness of column names and types"
        mapping: facts_all
        expected:
          - network IS PRESENT
          - xyz IS ABSENT
          - campaign IS OF TYPE (int,BIGINT)
          - lineitem IS OF TYPE float
```

Another example using the assertion inside a test:
```yaml
test:
  test_pricing:
    assertions:
      assert_pricing_columns:
        kind: columns
        description: "Assert correctness of column names and types"
        mapping: cube_pricing
        expected:
          - campaign IS OF TYPE (int,long)
          - lineitem IS OF TYPE string
          - imps IS OF TYPE long
          - price IS OF TYPE float 
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `columns`

* `description` **(optional)** *(type: string)*:
  A textual description of the assertion

* `mapping` **(optional)** *(type: string)*:
  The name of the mapping which is to be tested.

* `expected` **(optional)** *(type: list:string)*:
  A list of column schema expressions.
  

### Column Schema Expressions 

In order to verify type properties of columns, Flowman provides a small expression language. Currently, the following
four expressions a supported:

* `<column_name> IS PRESENT`: Verifies that a column called `column_name` is present in the schema. The check is
 case-insensitive.
* `<column_name> IS ABSENT`: Verifies that a column called `column_name` is *not* present in the schema. The check is
  case-insensitive.
* `<column_name> IS OF TYPE <typename>`: Verifies that a column called `column_name` is present in the schema and 
  that it is of type `typename`. All Spark SQL types are allowed. 
* `<column_name> IS OF TYPE (<typename_1>, <typename_2>, ...)`: Verifies that a column called `column_name` is present 
  in the schema and that it is of one of the types `typename_1`, `typename_2`, .... All Spark SQL types are allowed. 
