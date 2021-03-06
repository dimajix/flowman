# Delta Table Relations

The `deltaTable` relation is used for creating [Delta Lake](https://delta.io) tables stored in the Hive metastore. If
you want to use a Delta Lake table, but not store its meta data in Hive, then use the [`deltaFile` relation](deltaFile.md)
instead.

## Plugin

This relation type is provided as part of the [`flowman-delta` plugin](../../plugins/delta.md), which needs to be enabled in your
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.


## Example
```yaml
relations:
  some_delta_table:
    kind: deltaTable
    database: default
    table: financial_transactions
    # Specify the physical location where the data files should be stored at. If you leave this out, the Hive
    # default location will be used
    location: /warehouse/default/financial_transactions
    # Add partition column
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

## Fields
* `kind` **(mandatory)** *(string)*: `hiveTable`

* `schema` **(optional)** *(schema)* *(default: empty)*:
  Explicitly specifies the schema of the Delta Lake table.  Alternatively Flowman will automatically use the schema of
  the Hive table, if it already exists.

* `description` **(optional)** *(string)* *(default: empty)*:
  A description of the relation. This is purely for informational purpose.

* `options` **(optional)** *(map:string)* *(default: empty)*:
  All key-value pairs specified in *options* are directly passed to Apache Spark for reading
  and/or writing to this relation. The `options` will not be persisted in the Hive metastore. If that is what you
  want, then have a closer look at `properties` below.

* `database` **(mandatory)** *(string)*:
  Defines the Hive database where the table is defined. When no database is specified, the
  table is accessed without any specific qualification, meaning that the default database
  will be used.

* `table` **(mandatory)** *(string)*:
  Contains the name of the Hive table.

* `location` **(optional)** *(string)* *(default: empty)*:
  Specifies the location of the files stored in this Hive table. This setting is only used
  when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
  to the `LOCATION` in a `CREATE TABLE` statement.

* `partitions` **(optional)** *(list:partition)* *(default: empty)*:
  Specifies all partition columns. This is used both for creating Hive tables, but also for
  writing and reading to and from them. Therefore if you are working with partitioned Hive
  tables **you have to specify partition columns, even if Flowman is not used for creating
  the table**.

* `properties` **(optional)** *(map:string)* *(default: empty)*:
  Specifies additional properties of the Hive table. This setting is only used
  when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
  to the `TBLPROPERTIES` in a `CREATE TABLE` statement.


## Remarks

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.
