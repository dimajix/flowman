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
        description: "The measurement has to refer to an existing station"
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


## Column Checks

Flowman implements a couple of different check types on a per column basis. 

### Not NULL

One simple but yet important test is to check if a column does not contain any `NULL` values

* `kind` **(mandatory)** *(string)*: `notNull`
* `description` **(optional)** *(string)*:
An optional free text description to be shown in the documentation. This is a good place to provide the business
meaning of the data quality check.
* `filter` **(optional)** *(string)*:
Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
to exclude records with known quality issues.


### Unique Values

Another important test is to check for unique values in a column. Note that this test will exclude `NULL` values,
so in many cases you might want to specify both `notNUll` and `unique`.

* `kind` **(mandatory)** *(string)*: `unique`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### Foreign Key

A `foreignKey` column check is used to ensure that all not-`NULL` values refer to existing entries in a different
mapping or relation

* `kind` **(mandatory)** *(string)*: `foreignKey`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.
* `mapping` **(optional)** *(string)*: Name of mapping the foreign key refers to. You need to specify either the
  `mapping` or the `relation` property.
* `relation` **(optional)** *(string)*: Name of relation the foreign key refers to. You need to specify either the
  `mapping` or the `relation` property.
* `column`  **(optional)** *(string)*: Name of the column in the referenced entity (either mapping or relation). If
  this property is not set, then the same column name will be assumed


### Specific Values

In order to test if a column only contains specific values, you can use the `values` test.  Note that this test will 
exclude records with `NULL` values in the column, so in many cases you might want to specify both `notNUll` and `values`.

* `kind` **(mandatory)** *(string)*: `values`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `values` **(mandatory)** *(list:string)*: List of admissible values
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### Range of  Values

Especially when working with numerical data, you might also want to check their range. This can be implemented by using
the `range` test. Note that this test will exclude records with `NULL` values in the column, so in many cases you might
want to specify both `notNUll` and `range`.

* `kind` **(mandatory)** *(string)*: `range`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
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
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
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
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `expression` **(mandatory)** *(string)*: Boolean SQL Expression
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


## Schema Checks

In addition to checks for individual columns, Flowman also supports schema checks which may refer to multiple columns

### Primary Key
A `primaryKey` column check is used to ensure that all not-`NULL` values refer to existing entries in a different
mapping or relation

* `kind` **(mandatory)** *(string)*: `primaryKey`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.
* `columns`  **(optional)** *(list:string)*: Name of assumed primary key columns in the model


### Foreign Key
A `foreignKey` column check is used to ensure that all not-`NULL` values refer to existing entries in a different
mapping or relation

* `kind` **(mandatory)** *(string)*: `foreignKey`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.
* `mapping` **(optional)** *(string)*: Name of mapping the foreign key refers to. You need to specify either the
  `mapping` or the `relation` property.
* `relation` **(optional)** *(string)*: Name of relation the foreign key refers to. You need to specify either the
  `mapping` or the `relation` property.
* `columns`  **(optional)** *(list:string)*: Name of columns in the model
* `references`  **(optional)** *(list:string)*: Name of columns in the referenced entity


### SQL Expression
A very flexible test is provided with the SQL expression test. This test allows you to specify any simple SQL expression
(which may also use different columns), which should evaluate to `TRUE` for all records passing the test.

* `kind` **(mandatory)** *(string)*: `expression`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `expression` **(mandatory)** *(string)*: Boolean SQL Expression
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.


### SQL Query
A very flexible test is provided with the SQL query test. This test allows you to specify an arbitrary SQL `SELECT`
statement (which may also refer different mappings). The current entity is provided as `__THIS__`. The check actually
supports two different variants of queries, which differ in the interpretation of the result

#### Grouped Query
The first type of supported SQL queries returns multiple records, each having two columns (with arbitrary name). The 
first column should be a boolean indicating if the test succeeded, while the second column should be an integer 
containing the number of records. The names of the columns are irrelevant.

| Column | Data Type | Remark                                                |
|--------|-----------|-------------------------------------------------------|
| 1.     | `BOOL`    | Either`TRUE` or `FALSE` indicating success or failure |
| 2.     | `LONG`    | Number of records with `TRUE` or `FALSE` test result  |

Typically, a result set would contain two records, one with the first column `TRUE` and the second column holding the
number of records which passed the test and the second record having `FALSE` in the first column and the number of 
failed records in the second column.

The following example will check for the number of duplicates of the column `transaction_id`  
```yaml
kind: sql
query: |
  WITH dups AS (
    SELECT
      tx.transaction_id,
      COUNT(*) AS cnt
    FROM __this__ tx
    GROUP BY transaction_id
  )
  SELECT
    cnt = 1,
    COUNT(*)
  FROM dups
  GROUP BY 1
```

#### One-Record Query
The second type of supported SQL queries is required to return a single row that has to include one boolean column 
called `success`. The other columns are not interpreted by Flowman and only serve as informational columns.

The following query will compare the number of records in two mappings `raw_transactions` and `processed_transactions`.
The check succeeds if the numbers match, otherwise it fails. The number of records of each mapping is provided as 
additional values which will be shown in the documentation.
```yaml
kind: sql
query: |
  SELECT
    (SELECT COUNT(*) FROM raw_transactions) AS original_tx_count,
    (SELECT COUNT(*) FROM processed_transactions) AS final_tx_count,
    (SELECT COUNT(*) FROM raw_transactions) = (SELECT COUNT(*) FROM processed_transactions) AS success
```


* `kind` **(mandatory)** *(string)*: `sql`
* `description` **(optional)** *(string)*:
  An optional free text description to be shown in the documentation. This is a good place to provide the business
  meaning of the data quality check.
* `query` **(mandatory)** *(string)*: Boolean SQL Expression
* `filter` **(optional)** *(string)*:
  Optional SQL expression applied as a filter to select only a subset of all records for quality check. This is useful
  to exclude records with known quality issues.
