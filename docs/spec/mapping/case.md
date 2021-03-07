# Case Mapping

The `case` mapping can be used for a simple "switch/case" control flow, which selects a mapping depending on
given conditions.

## Example
```
mappings:
  switch:
    kind: case
    cases:
      - condition: ${processing_type == '1'}
        input: mapping_1
      - condition: ${processing_type == '2'}
        input: mapping_2:extra_output
      - condition: true
        input: default_mapping
```

## Fields
* `kind` **(mandatory)** *(string)*: `case`

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

* `cases` **(mandatory)** *(list)*:
  List of `condition` and `input` tuples. The first entry where `condition` evaluates to `true` will be selected.
  The inputs always refer to a specific mapping output.


## Outputs
* `main` - the only output of the mapping.


## Description

The `case` mapping works as a "switch/case" control flow, which selects one of the given alternative as the output.
The cases are processed in the order of the definition, and the first entry, which evaluates to `true` will be selected.
A default case may be added as the last entry by directly using `true` as a condition.

Note that all environment variables referenced in the conditions need to be defined, even if a case is not to be
selected.
