# Mock Relation

A `mock` relation works similar to a [`null`](null.md) relation in the sense that it does only return empty data.
The main difference is that a `mock` relation picks up the schema from a different relation. It's main use case is
within test cases where you want to replace physical data sources by empty mocked data sources with a minimum amount
of work.

## Example
```yaml
relations:
  mocked_relation:
    kind: mock
    relation: real_relation
```

```yaml
relations:
  some_relation:
    kind: mock
    relation: some_relation
    records:
        - [1,2,"some_string",""]
        - [2,null,"cat","black"]
```

```yaml
relations:
  data_raw:
    kind: mock
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
* `kind` **(mandatory)** *(string)*: `null` or `empty`

* `relation` **(optional)** *(string)* *(default: empty)*:
  Specify the base relation to be mocked. If no relation is specified, a relation with the same name will be mocked.
  Of course this doesn't work within the same project on project level. But it works well when the `mock` relation
  is created inside a test.

* `records` **(optional)** *(type: list:array)* *(default: empty)*:
  An optional list of records to be returned. Note that this list needs to include values for any partition columns
  of the mocked relation. The partition values need to be appended at the end.


## Output Modes
A `mock` relation supports all output modes, each of them simply discarding all records.


## Remarks

Mocking relations is very useful for creating meaningful tests. But you need to take into account one important fact:
Mocking for testing only works well if Flowman doesn't need to read real data. Access to real data might occur when
Flowman doesn't have all schema information in the specification and therefore falls back to Spark doing schema
inference. This is something you always should avoid. The best way to avoid automatic schema inference with Spark
is to explicitly specify schema definitions in all relations.
