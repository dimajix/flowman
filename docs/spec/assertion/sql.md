# SQL Assertion

One of the most flexible assertions is a *SQL query* assertion, which simply executed some SQL query, which can access
all mappings as inputs and then compares the result to some expected result.

## Example:

```yaml
kind: sql
description: "Assert the correct number of records in measurements_extracted"
query: "SELECT COUNT(*) FROM measurements_extracted"
expected: 2
```

You can also specify multiple assertions within a single `sql` assertion block:
```yaml
kind: sql
tests:
  - query: "SELECT id,count(*) FROM output_table GROUP BY id HAVING count(*) > 0"
    expected: []
  - query: "SELECT id,count(*) FROM output_cube GROUP BY id HAVING count(*) > 0"
    expected: []
```

Or a more complete example when used in conjunction with a [`verify`](../target/verify.md) target:
```yaml
targets:
  verify_output:
    kind: verify
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

* `kind` **(mandatory)** *(type: string)*: `sql`

* `description` **(optional)** *(type: string)*: 
  A textual description of the assertion

* `query` **(optional)** *(type: string)*: 
  The SQL query to be executed.

* `expected` **(optional)** *(type: list:record)*:
  The expected answer.
  
* `tests` **(optional)** *(type: map:test)*:
  An optional map of individual test cases, each containing a `query` and a `expected` field. Note that you can
  use the fields `query` together with `expected` for specifying a single check, or you can specify multiple tests
  in this list. You can also use a combination of both, although that might look strange.
