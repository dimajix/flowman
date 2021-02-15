# Null Relation

A `null` relation is a dummy relation, which can be used either for creating empty
data (but with a schema) during read operations or for throwing away records in
write operations. 

## Example
```yaml
relations:
  empty:
    kind: null
```

## Fields
* `kind` **(mandatory)** *(string)*: `null`

* `schema` **(optional)** *(schema)* *(default: empty)*:
  Explicitly specifies the schema of the null relation. 

* `description` **(optional)** *(string)* *(default: empty)*:
  A description of the relation. This is purely for informational purpose.

* `options` **(optional)** *(map:string)* *(default: empty)*:
  All options are passed directly to the reader/writer backend and are specific to each
  supported format.

* `partitions` **(optional)** *(list:partition)* *(default: empty)*:
  Even though a `null` relation does not provide any physical storage, it still optionally 
  provides virtual partition columns.
