
# Extend Mapping

The `extend` mapping will add new columns derived from the existing ones (or with constant
values) to a mapping. All incoming columns will be kept. If a specified column name matches an existing incoming
column name, then the column simply will be replaced with the new definition

## Example
```
mappings:
  facts_with_ids:
    kind: extend
    input: facts
    columns:
      placement_id: "CAST(placement AS INT)"
      lineitem_id: "CAST(SUBSTR(lineitem,5) AS INT)"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `extend`

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
Specifies the name of the input mapping.

* `columns` **(optional)** *(type: map:string)*:
Specifies a map of new column names with an expression for calculating them.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the transformation itself.


## Outputs
* `main` - the only output of the mapping


## Description
Essentially an `extend` mapping works like a simple SQL `SELECT` statement with all incoming
columns select by `*` and additional named columns. For example the mapping above would be
expressed in SQL as
```
SELECT
    *,
    CAST(placement AS INT) AS placement_id,
    CAST(SUBSTR(lineitem,5) AS INT) AS lineitem_id
FROM facts    
```
