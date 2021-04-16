# Project Mapping
The `project` mapping performs a *projection* of an input mapping onto a specific set of columns.
This corresponds to a simple SQL `SELECT` with a series of simple column names.

## Example
```yaml
mappings:
  partial_facts:
    kind: project
    input: facts
    columns:
      - id
      - temperature
      - wind_speed
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `project`

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
Specifies the list of columns to be present in the output. The list can either be simply a list of column names or
they can be more complex column descriptors
```yaml
columns:
 - name: name_of_output_column
   column: name_of_incoming_column
   type: string
```
You can also mix both column types in a single `project` mapping.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* projection.


## Outputs
* `main` - the only output of the mapping
