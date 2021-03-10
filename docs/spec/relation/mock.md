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

## Fields
* `kind` **(mandatory)** *(string)*: `null` or `empty`

* `relation` **(optional)** *(string)* *(default: empty)*:
  Specify the base relation to be mocked. If no relation is specified, a relation with the same name will be mocked.
  Of course this doesn't work within the same project on project level. But it works well when the `mock` relation
  is created inside a test.
  
