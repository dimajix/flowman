
# Flatten Mapping
The `flatten` mapping flattens all nested structure into a flat list of simple columns. The columns have their original
path encoded into their name, such that conflicts between same names in different sub trees are avoided. You can also
specify which naming schema to use when new column names are generated.

## Example
```
mappings:
  partial_facts:
    kind: flatten
    input: facts
    naming: snakeCase
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `conform`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE`
  * `DISK_ONLY`
  * `MEMORY_ONLY`
  * `MEMORY_ONLY_SER`
  * `MEMORY_AND_DISK`
  * `MEMORY_AND_DISK_SER`

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `naming` **(optional)** *(type: string)*:
Specifies the naming scheme used for the output. The following values are supported:
  * `camelCase` 
  * `snakeCase`
Note that the naming will only be used for concatenating column names and not for converting column names themselves.
This means that if your column names are using camel case and you specify `snakeCase` then the path elements are left
unchanged but concatenated using an underscore (`_`).

## Description
