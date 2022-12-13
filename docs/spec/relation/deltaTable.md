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
* `kind` **(mandatory)** *(string)*: `deltaTable`

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
  Specifies the location of the files stored in this Hive table. This setting is only used when Flowman is used to 
  create the Delta table within Hive table and is ignored otherwise. This corresponds to the `LOCATION` in a 
  `CREATE TABLE` statement.

* `partitions` **(optional)** *(list:partition)* *(default: empty)*:
  Specifies all partition columns. This is used both for creating Hive tables, but also for writing and reading to and
  from them. Therefore if you are working with partitioned Hive tables **you have to specify partition columns, even 
  if Flowman is not used for creating the table**. Normally the partition columns are separate from the
  schema, but you *may* also include the partition column in the schema, although this is not considered to be best
  practice. But it turns out to be quite useful in combination with dynamically writing to multiple partitions.

* `properties` **(optional)** *(map:string)* *(default: empty)*:
  Specifies additional properties of the Hive table. This setting is only used
  when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
  to the `TBLPROPERTIES` in a `CREATE TABLE` statement.

* `mergeKey` **(optional)** *(list:string)* *(default: empty)*:
  List of column names specifying the key to identify matching records on `update` operations.

* `migrationPolicy` **(optional)** *(string)* *(default: empty)*
  Can be one of `RELAXED` or `STRICT`. If left empty, then the value of the Flowman configuration property
  `flowman.default.relation.migrationPolicy` will be used instead.

* `migrationStrategy` **(optional)** *(string)* *(default: empty)*
  Can be one of `ALTER`, `ALTER_REPLACE`, `REPLACE`, `NEVER` or `FAIL`. If left empty, then the value of the Flowman
  configuration property `flowman.default.relation.migrationStrategy` will be used instead.


## Automatic Migrations
Flowman supports some [automatic migrations](../../concepts/migrations.md), specifically with the migration strategies 
`ALTER`, `ALTER_REPLACE` and `REPLACE` (those can be set via the property `migrationStrategy` or the global config 
variable `flowman.default.relation.migrationStrategy`, see [configuration](../../setup/config.md) for more details).

The migration strategy `ALTER` supports the following alterations:
* Changing nullability
* Changing the comment
* Adding new columns

Other changes (like changing the data type or dropping columns) is not supported in the `ALTER` strategy and
will require either `REPLACE` or `ALTER_REPLACE` - but this will remove all existing data in that table!


## Schema Conversion
The Delta table relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).


## Output Modes

### Batch Writing
The `deltaTable` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                                                                                  |
|---------------------|-----------|-----------------------------------------------------------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the Delta table already exists                                                          |
| `ignoreIfExists`    | yes       | Do nothing if the Delta table already exists                                                              |
| `overwrite`         | yes       | Overwrite the whole table or the specified partitions                                                     |
| `overwrite_dynamic` | yes       | Overwrites all partitions where records are provided in the input data. Other partitions remain untouched |
| `append`            | yes       | Append new records to the existing table                                                                  |
| `update`            | yes       | Updates existing records, either using `mergeKey` or the primary key of the specified `schema`            |

In addition, the `deltaFile` relation also supports complex merge operations in a [`merge` target](../target/merge.md).

Note that support `overwrite_dynamic` is not perfect and is implemented in a two phase approach: First all partition
values are calculated, then the data is inserted. This implies that the input data is scanned twice, which might be an
expensive operation depending on the computational complexity and the amount of data.


### Stream Writing
In addition to batch writing, the Delta table relation also supports stream writing via the
[`stream` target](../target/stream.md) with the following semantics:

| Output Mode | Supported | Comments                                                                      |
|-------------|-----------|-------------------------------------------------------------------------------|
| `append`    | yes       | Append new records from the streaming process once they don't change any more |
| `update`    | yes       | Append records every time they are updated                                    |
| `complete`  | yes       | -                                                                             |


## Remarks

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.

### Writing to Dynamic Partitions

Beside explicitly writing to a single Hive partition, Flowman also supports to write to multiple partitions where
the records need to contain values for the partition columns. Note that currently Delta table do not support the
relation output mode `dynamic_overwrite` with dynamic partitions, instead you can only use `append` and `overwrite`. 
This means, that whenever you write to a partitioned Delta table without explicitly specifying the target partition 
in the [relation target](../target/relation.md), then the table is truncated first and all normally unchanged partitions 
will be lost.
