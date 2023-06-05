# Cast Mapping

The `cast` mapping is a simple way to change the data type of individual columns without needing to specify all
columns. In many cases, you could also use the [`extend`](extend.md) with SQL `CAST` expressions to achieve the
same result, but the later one does not support `VARCHAR(n)` and `CHAR(n)` data types.

## Example

```yaml
kind: cast
input: some_mapping
columns:
  id: "CHAR(12)"
  amount: "DECIMAL(16,3)"
```


## Fields
* `kind` **(mandatory)** *(string)*: `cast`

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

* `input` **(mandatory)** *(string)*:
  The name of the input mapping

* `columns` **(mandatory)** *(map:type)*:
Map of column names to desired data types. All data types supported by Apache Spark can be used here.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the transformation itself.

## Outputs
* `main` - the only output of the mapping


## Remarks

In contrast to the [`project` mapping](project.md), you only need to specify those columns where you want to change the
data type. All other columns will be passed through without any change. Also note that the column order of the output
is the same as of the input mapping.

The differences to the [`project`](project.md) mapping and the [`schema`](schema.md) are as follows:
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
