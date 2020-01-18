
# Read Mapping


## Example
```
mappings:
  measurements-raw:
    kind: readRelation
    relation: measurements-raw
    partitions:
      year:
        start: $start_year
        end: $end_year
    columns:
      raw_data: String
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `read` or `readRelation`

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

* `relation` **(mandatory)** *(type: string)*:
Specifies the name of the relation to read from

* `partitions` **(optional)** *(type: map:partition)*:
Specifies the partition (or multiple partitions) to read data from

* `columns` **(optional)** *(type: map:data_type)* *(default: empty):
Specifies the list of columns and types to read from the relation. This schema
will be applied to the records after they have been read and interpreted by the
underlying source.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied for reading only a subset of records.


## Outputs
* `main` - the only output of the mapping


## Description
