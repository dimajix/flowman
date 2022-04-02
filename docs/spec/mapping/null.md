# Null Mapping

The `null` mapping is a dummy mapping which produces and empty output but with a specified schema. This
is mainly useful for mocking other mappings in tests.

## Example
```yaml
mappings:
  empty_mapping:
    kind: null
    columns:
      id: String
      temperature: Float
      wind_speed: Float
```

```yaml
mappings:
  empty_mapping:
    kind: null
    schema:
      kind: embedded
      fields:
        - name: id
          type: string
        - name: amount
          type: double
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `null`

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

* `columns` **(optional)** *(type: map:string)*:
  Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
  As an alternative of specifying a list of columns you can also directly specify a schema.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* schema operation.


## Outputs
* `main` - the only output of the mapping

