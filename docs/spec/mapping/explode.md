
# Explode Mapping

The `explode` mapping is used for creating multiple records from an array field. The mapping has
been specifically designed to support extraction of nested sub-entities of complex records containing
nested entities.

## Example
```
mappings:
  sub_entity:
    kind: explode
    input: events
    array: data.sub_entity
    flatten: true
    outerColumns:
      # Drop everything from parent entity, except...
      drop: "*"
      # Only keep some important fields from parent entity
      rename:
        data_operation: dataOperation
        parent_id: data.id
    innerColumns:
      # Remove all arrays again
      drop:
        - subEntityList
        - someConfigList
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `explode`

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
Specify the name of the input mapping

* `array` **(mandatory)** *(type: string)*:
Specify the column containing the array to be exploded.

* `flatten` **(optional)** *(type: boolean)* *(default: `false`)*:
The mapping can optionally flatten all nested structures of the result.

* `naming` **(optional)** *(type: string)* *(default: `snakeCase`)*:
The optional flattening can either use a `snakeCase` or `camelCase` naming strategy for deriving
the final column names.

* `outerColumns` **(optional)**
The `outerColumns` object specifies how to proceed with columns from the enclosing structure of the
array, i.e. all columns which are not part of the array itself. You can either drop some columns,
keep columns or also rename columns.
  * `drop` **(optional)** *(type: list)*
  * `keep` **(optional)** *(type: list)*
  * `rename` **(optional)** *(type: map)*
  
* `innerColumns`
The `innerColumns` object specifies how to proceed with columns inside the array to be exploded. 
You can either drop some columns, keep columns or also rename columns.
  * `drop` **(optional)** *(type: list)*
  * `keep` **(optional)** *(type: list)*
  * `rename` **(optional)** *(type: map)*
  

## Description
