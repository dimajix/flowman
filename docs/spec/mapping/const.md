# Const Mapping

A `const` mapping contains directly specified constant values. It is a good candidate to be used for mocking data in
tests.


## Example

```yaml
mappings:
  fake_input:
    kind: const  
    schema:
      kind: embedded
      fields:
        - name: str_col
          type: string
        - name: int_col
          type: integer
    records:
        - [1,"some_string"]
        - [2,"cat"]
```

```yaml
mappings:
  fake_input:
    kind: const
    columns:
      str_col: string
      int_col: integer
    records:
        - [1,"some_string"]
        - [2,"cat"]
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `const`

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

* `records` **(optional)** *(type: list:array)* *(default: empty)*:
  An optional list of records to be returned.

* `columns` **(optional)** *(type: map:string)*:
  Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
  As an alternative of specifying a list of columns you can also directly specify a schema.


## Outputs
* `main` - the only output of the mapping
