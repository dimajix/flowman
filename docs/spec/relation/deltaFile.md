# Delta File Relations

The `deltaFile` relation is used for creating [Delta Lake](https://delta.io) tables stored only in some Hadoop 
compatible file system, without registering the table in the Hive metastore. If you want to use a Delta Lake table,
and you also want to store its meta data in Hive, then use the [`deltaFile` relation](deltaTable.md) instead.

## Plugin

This relation type is provided as part of the [`flowman-delta` plugin](../../plugins/delta.md), which needs to be enabled in your
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.

## Example
```yaml
relations:
  some_delta_table:
    kind: deltaFile
    # Specify the physical location where the data files should be stored at.
    location: /warehouse/default/financial_transactions
    # Optional add partition column
    partitions:
      - name: business_date
        type: string
    # Specify the default key to use in upsert operations. Normally this should match the primary key 
    # (except partition columns, which will be added implicitly)
    mergeKey:
      - id
    # Specify a schema, which is mandatory for creating the table during CREATE phase
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: double
```

## Remarks

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.
