# Mock Mapping

The `mock` mapping works similar to the [`null`](null.md) mapping in that it creates an empty output. But instead of
explicitly specifying a schema of the empty output, the `mock` mapping will fetch the schema from a different mapping.
This mapping is most useful to be used in tests. In addition it is also possible to manually specify records to be
returned, which makes this mapping even more convenient for mocking.

## Example
```yaml
mappings:
  # This will mock `some_mapping`
  some_mapping:
    kind: mock
```

```yaml
mappings:
  some_other_mapping:
    kind: mock
    mapping: some_mapping
    records:
      - [1,2,"some_string",""]
      - [2,null,"cat","black"]
```

```yaml
mappings:
  some_mapping:
    kind: mock
    mapping: some_mapping
    records:
      - Campaign ID: DIR_36919
        LineItemID ID: DIR_260390
        SiteID ID: 23374
        CreativeID ID: 292668
        PlacementID ID: 108460
      - Campaign ID: DIR_36919
        LineItemID ID: DIR_260390
        SiteID ID: 23374
        CreativeID ID: 292668
        PlacementID ID: 108460
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
  Specifies the name of the mapping to be mocked. If no name is given, then a mapping with the same name will be 
  mocked. Note that this will only work when used as an override mapping in test cases, otherwise an infinite loop
  would be created by referencing to itself.

* `records` **(optional)** *(type: list:array)* *(default: empty)*:
  An optional list of records to be returned.  


## Outputs
The `mock` mapping provides all outputs as the mocked mapping.


## Remarks

Mocking mappings is very useful for creating meaningful tests. But you need to take into account one important fact:
Mocking for testing only works well if Flowman doesn't need to read real data. Access to real data might occur when
Flowman doesn't have all schema information in the specification and therefore falls back to Spark doing schema
inference. This is something you always should avoid. The best way to avoid automatic schema inference with Spark
is to explicitly specify schema definitions in all relations and/or to explicitly specify all columns with data types
in the [`readRelation`](read-relation.md) mapping.
