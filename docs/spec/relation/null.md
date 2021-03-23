# Null Relation

A `null` relation is a dummy relation, which can be used either for creating empty
data (but with a schema) during read operations or for throwing away records in
write operations. 

## Example
```yaml
relations:
  empty:
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
* `kind` **(mandatory)** *(string)*: `null` or `empty`

* `schema` **(optional)** *(schema)* *(default: empty)*:
  Explicitly specifies the schema of the null relation. 

* `description` **(optional)** *(string)* *(default: empty)*:
  A description of the relation. This is purely for informational purpose.

* `partitions` **(optional)** *(list:partition)* *(default: empty)*:
  Even though a `null` relation does not provide any physical storage, it still optionally 
  provides virtual partition columns.
