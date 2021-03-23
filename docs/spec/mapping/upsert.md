# Upsert Mapping

The `upsert` mapping is used to merge two data sets using upsert logic. That means that updates are inserted into an
existing data set and replace existing entries. Entries are identified via primary key columns, which need to 
be specified as part of this mapping

## Example
```yaml
mappings:
  merge_updates:
    kind: upsert
    input: previous_state
    updates: state_updates
    filter: "operation != 'DELETE'"
    keyColumns: id
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `upsert`

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

* `input` **(required)** *(type: string)*:
  Name of the input mapping containing  the previous state without any updates.
  
* `updates` **(required)** *(type: string)*:
  Name of the additional mapping which contains updates and new entries.
  
* `keyColumn` **(required)** *(type: list:string)*
  List of column names which form a primary key used for merging.
  
* `filter` **(optional)** *(type: string)*
  Optional filter condition, which will be applied after the updates have been merged into the input data set. This
  filter can be used to remove deleted entries, for example.
  

## Outputs
* `main` - the only output of the mapping
