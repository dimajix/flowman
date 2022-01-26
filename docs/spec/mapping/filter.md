# Filter Mapping

The `filter` mapping is one of the simplest one and applies a row filter to all incoming records. This is equivalent
to a `WHERE` or `HAVING` condition in a classical SQL statement.

## Example
```yaml
mappings:
  facts_special:
    kind: filter
    input: facts_all
    condition: "special_flag = TRUE"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `filter`

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

* `condition` **(mandatory)** *(type: string)*:
Specifies the condition as a SQL expression to filter on


## Outputs
* `main` - the only output of the mapping


## Description
The `filter` mapping essentially corresponds to a SQL `WHERE` or `HAVING` clause. The example
above would be equivalent to the following SQL statement:
```
SELECT
    *
FROM facts_all
WHERE special_flag=TRUE
```
