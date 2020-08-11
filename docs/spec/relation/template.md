# Flowman Relation Template

## Example

```yaml
relations:
  structured_macro:
    kind: hiveUnionTable
    viewDatabase: "dqm"
    view: "${table}"
    tableDatabase: "dqm"
    tablePrefix: "zz_${table}"
    locationPrefix: "$hdfs_structured_dir/dqm/zz_${table}"
    external: true
    format: parquet
    partitions:
    - name: landing_date
      type: string
    schema:
      kind: mapping
      mapping: ${schema}

  fee:
    kind: template
    relation: structured_macro
    environment:
    - table=fee
    - schema=fee
```
