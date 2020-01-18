
# Union Mapping

## Example

```
mappings:
  facts_all:
    kind: union
    inputs:
      - facts_rtb
      - facts_direct
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `union`

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

* `inputs` **(mandatory)** *(type: list:string)*:
List of input mappings to build the union of.

* `columns` **(optional)** *(type: map:string)* *(default: empty)*:
Optionally you can specify the list a list of columns. Then the union will only
contain these columns, otherwise the superset of all columns of all input mappings
will be used.

* `distinct` **(optional)** *(type: boolean)* *(default: false)*:
 If set to true, only distinct records will be returned (using the specified or inferred set 
 of columns).

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* the union operation.


## Outputs
* `main` - the only output of the mapping


## Description

Essentially the `union` mapping performs a SQL `UNION ALL`. In contrast to most SQL 
implementations, the `union` mapping actually uses column names instead of column positions
for matching multiple input mappings. 

Optionally you can also set `distinct` to true, then the operation corresponds to a SQL
`UNION DISTINCT`
