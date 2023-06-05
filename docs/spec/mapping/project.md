# Project Mapping
The `project` mapping performs a *projection* of an input mapping onto a specific set of columns.
This corresponds to a trivial SQL `SELECT` with a series of simple column names, with optional `CAST` operations.


## Example
The `project` mapping supports two different syntax. The first concise version only selects specific columns:
```yaml
mappings:
  partial_facts:
    kind: project
    input: facts
    columns:
      - id
      - temperature
      - wind_speed
```
The second version also supports changing the column name and data type and optional description:
```yaml
mappings:
  partial_facts:
    kind: project
    input: facts
    columns:
      - column: id
      - column: air_temperature
        name: temperature
        type: FLOAT
        description: "The air temperature"
      - column: wind_speed
        type: FLOAT
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `project`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE` - Disables caching of teh results of this mapping
  * `DISK_ONLY` - Caches the results on disk
  * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
  * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
  * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
  * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `columns` **(mandatory)** *(type: list:string)*:
Specifies the list of columns to be present in the output. The list can either be simply a list of column names or
they can be more complex column descriptors
```yaml
columns:
 - name: name_of_output_column
   column: name_of_incoming_column
   type: string
   description: "This is the (optional) description of the column"
```
You can also mix both column types in a single `project` mapping.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* projection.


## Outputs
* `main` - the only output of the mapping


## Remarks
The `project` mapping is similar to both the [`cast`](cast.md) mapping and the [`schema`](schema.md) mapping. The 
differences are as follows:
* The [`project` mapping](project.md) only performs simple type conversions and will only emit the columns specified
in the `columns` list. It will drop all columns not specified in the list and cannot add new columns.
* The [`schema` mapping](schema.md) allows specifying arbitrary complex data types including nested types. It will
make sure that the result precisely matches the specified schema, i.e. it will add and/or drop columns as required.
* The [`cast` mapping](cast.md) will only change the data type of the specified columns and will keep all other columns
unchanged. This means that it will not add or drop any of the incoming columns.

### Supported data types

The following simple data types are supported by Apache Spark and Flowman:

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
