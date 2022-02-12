# Testing Model Properties

In addition to provide pure descriptions of model entities, the documentation framework in Flowman also provides
the ability to specify model properties (like unqiue values in a column, not null etc). These properties will not only
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
          tests:
            - kind: notNull
        - name: usaf
          tests:
            - kind: notNull
        - name: wban
          tests:
            - kind: notNull
        - name: air_temperature_qual
          tests:
            - kind: notNull
            - kind: values
              values: [0,1,2,3,4,5,6,7,8,9]
```

## Available Column Tests

Flowman implements a couple of different test cases on a per column basis. 

### Not NULL

One simple but yet important test is to check if a column does not contain any `NULL` values

* `kind` **(mandatory)** *(string)*: `notNull`


### Unique Values

Another important test is to check for unique values in a column. Note that this test will exclude `NULL` values,
so in many cases you might want to specify both `notNUll` and `unique`.

* `kind` **(mandatory)** *(string)*: `unique`


### Specific Values

In order to test if a column only contains specific values, you can use the `values` test.  Note that this test will 
exclude records with `NULL` values in the column, so in many cases you might want to specify both `notNUll` and `values`.

* `kind` **(mandatory)** *(string)*: `values`
* `values` **(mandatory)** *(list:string)*: List of admissible values


### Range of  Values

Especially when working with numerical data, you might also want to check their range. This can be implemented by using
the `range` test. Note that this test will exclude records with `NULL` values in the column, so in many cases you might
want to specify both `notNUll` and `range`.

* `kind` **(mandatory)** *(string)*: `range`
* `lower` **(mandatory)** *(string)*: Lower value (inclusive)
* `upper` **(mandatory)** *(string)*: Upper value (inclusive)
