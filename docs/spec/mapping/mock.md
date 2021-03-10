# Mock Mapping

The `mock` mapping works similar to the [`null`](null.md) mapping in that it creates an empty output. But instead of
explicitly specifying a schema of the empty output, the `mock` mapping will fetch the schema from a different mapping.
This mapping is most useful to be used in tests.

## Example
```yaml
mappings:
  # This will mock `some_mapping`
  some_mapping:
    kind: mock
```

```yaml
mappings:
  empty_mapping:
    kind: mock
    mapping: some_mapping
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `mock`

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

* `mapping` **(optional)** *(type: string)*:
  Specifies the name of the mapping to be mocked. If no name is given, the a mapping with the same name will be 
  mocked. Note that this will only work when used as an override mapping in test cases, otherwise an infinite loop
  would be created by referencing to itself.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* schema operation.


## Outputs
The `mock` mapping provides all outputs as the mocked mapping.
