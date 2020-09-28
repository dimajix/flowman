# Drop Mapping
The `drop` mapping performs a *projection* of an input mapping by dropping a given list of columns.

## Example
```yaml
mappings:
  partial_facts:
    kind: drop
    input: facts
    columns:
      - id
      - temperature
      - wind_speed
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `drop`

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
Specifies the list of columns to be removed from the input.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* projection.


## Outputs
* `main` - the only output of the mapping
