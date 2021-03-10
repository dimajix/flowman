
# Schema Mapping
The `schema` mapping performs a *projection* of an input mapping onto a specific set of columns
and also performs type conversions. This corresponds to a simple SQL `SELECT` with a series of
`CAST` expressions.

## Example
```yaml
mappings:
  partial_facts:
    kind: schema
    input: facts
    columns:
      id: String
      temperature: Float
      wind_speed: Float
```

```yaml
mappings:
  partial_facts:
    kind: schema
    input: facts
    schema:
      kind: embedded
      fields:
        - name: id
          type: string
        - name: amount
          type: double
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `schema`

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

* `schema` **(optional)** *(type: schema)*:
As an alternative of specifying a list of columns you can also directly specify a schema.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* schema operation.


## Outputs
* `main` - the only output of the mapping


## Description
