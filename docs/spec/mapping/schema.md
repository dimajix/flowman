
# Schema Mapping
The `schema` mapping performs a *projection* of an input mapping onto a specific set of columns
and also performs type conversions. This corresponds to a simple SQL `SELECT` with a series of
`CAST` expressions. The main difference is that fields, which are missing from the mappings input, are silently
added with `NULL` values. A typical use case of this mapping is to conform data from possibly slightly different
sources to a single super-schema.

## Example
```yaml
mappings:
  partial_facts:
    kind: schema
    input: facts
    columns:
      id: String
      temperature: Float
      wind_speed: Float
```

```yaml
mappings:
  partial_facts:
    kind: schema
    input: facts
    columnMismatchPolicy: ADD_REMOVE_COLUMNS
    typeMismatchPolicy: CAST_ALWAYS
    charVarcharPolicy: PAD_AND_TRUNCATE
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: double
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `schema`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE`
  * `DISK_ONLY`
  * `MEMORY_ONLY`
  * `MEMORY_ONLY_SER`
  * `MEMORY_AND_DISK`
  * `MEMORY_AND_DISK_SER`

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `columns` **(optional)** *(type: map:string)*:
Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
As an alternative of specifying a list of columns you can also directly specify a schema, as described in
[schema](../schema/index.md).

* `columnMismatchPolicy` **(optional)** *(type: string)* *(default: `ADD_REMOVE_COLUMNS`)*:
Control how Flowman will handle a mismatch between column names in the source and the provided schema:
  - `IGNORE` will simply pass through the input columns unchanged
  - `ERROR` will fail the build once a mismatch between actual and requested schema is detected
  - `ADD_COLUMNS_OR_IGNORE` will add (`NULL`) columns from the requested schema to the input schema, and will keep columns in the input schema which are not present in the requested schema
  - `ADD_COLUMNS_OR_ERROR` will add (`NULL`) columns from the requested schema to the input schema, but will fail the build if the input schema contains columns not present in the requested schema
  - `REMOVE_COLUMNS_OR_IGNORE` will remove columns from the input schema which are not present in the requested schema
  - `REMOVE_COLUMNS_OR_ERROR` will remove columns from the input schema which are not present in the requested schema and will fail if the input schema is missing requested columns
  - `ADD_REMOVE_COLUMNS` will essentially pass through the requested schema as is (the default)

* `typeMismatchPolicy` **(optional)** *(type: string)* *(default: `CAST_ALWAYS`)*:
Control how Flowman will convert between data types:
  - `IGNORE` - Ignores any data type mismatches and does not perform any conversion
  - `ERROR` - Throws an error on data type mismatches
  - `CAST_COMPATIBLE_OR_ERROR` - Performs a data type conversion with compatible types, otherwise throws an error
  - `CAST_COMPATIBLE_OR_IGNORE` - Performs a data type conversion with compatible types, otherwise does not perform conversion
  - `CAST_ALWAYS` - Always performs data type conversion (the default)

* `charVarcharPolicy` **(optional)** *(type: string)* *(default: `PAD_AND_TRUNCATE`)*:
Control how Flowman will treat `VARCHAR(n)` and `CHAR(n)` data types. The possible values are
  - `IGNORE` - Do not apply any length restrictions
  - `PAD_AND_TRUNCATE` - Truncate `VARCHAR(n)`/`CHAR(n)` strings which are too long and pad `CHAR(n)` strings which are too short
  - `PAD` - Pad `CHAR(n)` strings which are too short
  - `TRUNCATE` - Truncate `VARCHAR(n)`/`CHAR(n)` strings which are too long

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* schema operation.


## Outputs
* `main` - the only output of the mapping


## Remarks
The `schema` mapping is similar to both the [`cast`](cast.md) mapping and the [`project`](project.md) mapping. The
differences are as follows:
* The [`project` mapping](project.md) only performs simple type conversions and will only emit the columns specified
  in the `columns` list. It will drop all columns not specified in the list and cannot add new columns.
* The [`schema` mapping](schema.md) allows specifying arbitrary complex data types including nested types. It will
  make sure that the result precisely matches the specified schema, i.e. it will add and/or drop columns as required.
* The [`cast` mapping](cast.md) will only change the data type of the specified columns and will keep all other columns
  unchanged. This means that it will not add or drop any of the incoming columns.



### Supported data types

The following simple data types are supported for the `columns` property:

* `string`, `text` - text and strings of arbitrary length
* `binary` - binary data of arbitrary length
* `tinyint`, `byte` - 8-bit signed numbers
* `smallint`, `short` - 16-bit signed numbers
* `int`, `integer` - 32-bit signed numbers
* `bigint`, `long` - 64-bit signed numbers
* `boolean` - true or false
* `float` - 32-bit floating point number
* `double` - 64-bit floating point number
* `decimal(a,b)`
* `varchar(n)` - text with up to `n` characters. Note that this data type is only supported for specifying input or
  output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length.
* `char(n)` - text with exactly `n` characters. Note that this data type is only supported for specifying input or
  output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length.
* `date` - date type
* `timestamp` - timestamp type (date and time)
* `duration` - duration type
