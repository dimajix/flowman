
# Schema Mapping
The `schema` mapping performs a *projection* of an input mapping onto a specific set of columns
and also performs type conversions. This corresponds to a simple SQL `SELECT` with a series of
`CAST` expressions.

## Example
```
mappings:
  partial_facts:
    kind: conform
    input: facts
    columns:
      id: String
      temperature: Float
      wind_speed: Float
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

* `columns` **(optional)** *(type: map:string)*:
Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: string)*:
As an alternative of specifying a list of columns you can also directly specify a schema.


## Description
