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
Map of column names to desired data types

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the transformation itself.


## Remarks

In contrast to the [`project` mapping](project.md), you only need to specify those columns where you want to change the
data type. All other columns will be passed through without any change. Also note that the column order of the output
is the same as of the input mapping.
