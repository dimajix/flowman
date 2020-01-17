
# Coalesce Mapping

Reduces the number of Spark partitions by logically merging partitions together.  
This will not perform any shuffle operation.

## Example
```
mappings:
  facts_bidding:
    kind: coalesce
    input: facts_all
    partitions: 32
```

## Fields
* `kind` **(mandatory)** *(string)*: `coalesce`

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

* `partitions` **(mandatory)** *(integer)*:
The number of output partitions


## Description
