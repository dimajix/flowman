# Template Relation

## Example

```yaml
relations:
  # First define the template relation itself  
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

  # Now use the template and replace some of the used variables
  fee:
    kind: template
    relation: structured_macro
    environment:
    - table=fee
    - schema=fee
```
