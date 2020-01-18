
# Latest Mapping

The `latest` mapping keeps only the latest (newest) record per ID. This is useful
when working with streams of change events and you only want to keep the newest
event for each ID.

## Example
```
mappings:
  latest_customer_updates:
    kind: latest
    input: all_customer_updates
    versionColumn: ts
    keyColumns: customer_id
```

## Fields
* `kind` **(mandatory)** *(string)*: `latest`

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

* `versionColumn`
Specifies the column where the version (or timestamp) is contained. For each ID only
the record with the highest value will be kept.

* `keyColumns`
Specifies one or more columns forming a primary key or ID. Different versions of the
same entity are then distinguished by the `versionColumn` 


## Outputs
* `main` - the only output of the mapping
