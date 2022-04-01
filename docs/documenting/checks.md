# Checking Model Properties

In addition to provide pure descriptions of model entities, the documentation framework in Flowman also provides
the ability to specify model properties (like unique values in a column, not null etc). These properties will not only
be part of the documentation, they will also be verified as part of generating the documentation.


## Example

```yaml
relations:
  measurements:
    kind: file
    format: parquet
    location: "$basedir/measurements/"
    partitions:
      - name: year
        type: integer
        granularity: 1
    # We prefer to use the inferred schema of the mapping that is written into the relation
    schema:
      kind: mapping
      mapping: measurements_extracted

    documentation:
      description: "This model contains all individual measurements"
      columns:
        - name: year
          description: "The year of the measurement, used for partitioning the data"
          checks:
            # Check that the column does not contain NULL values
            - kind: notNull
        - name: usaf
          checks:
            # Check for NOT NULL values, but exclude known defects
            - kind: notNull
              filter: "year >= 2000"
        - name: wban
          checks:
            # Check for NOT NULL values, but exclude known defects
            - kind: notNull
              filter: "usaf NOT IN (SELECT usaf FROM known_defective_usaf)"
        - name: air_temperature_qual
          checks:
            - kind: notNull
            # Check that the column only contains the specified values
            - kind: values
              values: [0,1,2,3,4,5,6,7,8,9]
        - name: air_temperature
          checks:
            # Perform an arbitrary check on the column, you can also access other columns
            - kind: expression
              expression: "air_temperature >= -100 OR air_temperature_qual <> 1"
            - kind: expression
              expression: "air_temperature <= 100  OR air_temperature_qual <> 1"
      # Schema tests, which might involve multiple columns
      checks:
        # Check that each usaf/wban combination is a foreign key refering to the "stations" relation          
        kind: foreignKey
        relation: stations
        columns:
          - usaf
          - wban
        references:
          - usaf
          - wban

mappings:
  # Provide list of known defects to be excluded from some checks
  known_defective_usaf:
    kind: values
    columns:
      usaf: string
      records:
        - ["12345"]
        - ["87600"]
```

## Available Column Checks

Flowman implements a couple of different check types on a per column basis. 

### Not NULL

One simple but yet important test is to check if a column does not contain any `NULL` values

* `kind` **(mandatory)** *(string)*: `notNull`
* `filter` **(optional)** *(string)*:
Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
to exclude records with known quality issues.


### Unique Values

Another important test is to check for unique values in a column. Note that this test will exclude `NULL` values,
so in many cases you might want to specify both `notNUll` and `unique`.

* `kind` **(mandatory)** *(string)*: `unique`
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### Specific Values

In order to test if a column only contains specific values, you can use the `values` test.  Note that this test will 
exclude records with `NULL` values in the column, so in many cases you might want to specify both `notNUll` and `values`.

* `kind` **(mandatory)** *(string)*: `values`
* `values` **(mandatory)** *(list:string)*: List of admissible values
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### Range of  Values

Especially when working with numerical data, you might also want to check their range. This can be implemented by using
the `range` test. Note that this test will exclude records with `NULL` values in the column, so in many cases you might
want to specify both `notNUll` and `range`.

* `kind` **(mandatory)** *(string)*: `range`
* `lower` **(mandatory)** *(string)*: Lower value (inclusive)
* `upper` **(mandatory)** *(string)*: Upper value (inclusive)
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### Length

When working with string data, you might also want to check their length. This can be implemented by using
the `length` test. Note that this test will exclude records with `NULL` values in the column, so in many cases you might
want to specify both `notNUll` and `range`.

* `kind` **(mandatory)** *(string)*: `length`
* `minimum` **(optional)** *(int)*: Minimum length (inclusive)
* `maximum` **(optional)** *(int)*: Maximum length (inclusive)
* `length` **(optional)** *(int)*: Exact length (will set minimum and maximum)
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### SQL Expression

A very flexible test is provided with the SQL expression test. This test allows you to specify any simple SQL expression
(which may also use different columns), which should evaluate to `TRUE` for all records passing the test.

* `kind` **(mandatory)** *(string)*: `expression`
* `expression` **(mandatory)** *(string)*: Boolean SQL Expression
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.
