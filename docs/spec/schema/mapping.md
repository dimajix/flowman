# Mapping Schema

The `mapping` schema is used to infer a schema from a given mapping. This way, the schema of outgoing relations can
be implicitly specified by referencing the mapping that will be written to the relation.

## Example

```yaml
relations:
  output:
    kind: hiveTable
    database: "crm"
    table: "customers"
    format: parquet
    partitions:
    - name: landing_date
      type: string
    schema:
      kind: mapping
      mapping: customers
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `mapping`
* `mapping` **(mandatory)** *(type: string)*:
Specifies the name of mapping of which the schema should be used.
