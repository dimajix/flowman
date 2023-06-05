# SQL Server Relations

The SQL Server relation allows you to access MS SQL Server and Azure SQL databases using a JDBC driver. It uses the
`spark-sql-connector` from Microsoft to speed up processing. The `sqlserver` relation will also make use of a 
global temporary table as an intermediate staging target and then atomically replace the contents of the target
table with the contents of the temp table within a single transaction.


## Plugin

This relation type is provided as part of the [`flowman-mssql` plugin](../../plugins/mssqlserver.md), which needs to be enabled 
in your `namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.


## Example

```yaml
# First specify a connection. This can be used by multiple SQL Server relations
connections:
  frontend:
    kind: jdbc
    url: "$frontend_db_url"
    username: "$frontend_db_username"
    password: "$frontend_db_password"

relations:
  frontend_users:
    kind: sqlserver
    # Specify the name of the connection to use
    connection: frontend
    # Specify the table
    table: "users"
    schema:
      kind: avro
      file: "${project.basedir}/schema/users.avsc"
    primaryKey:
      - user_id
    indexes:
      - name: "users_idx0"
        columns: [user_first_name, user_last_name]
```
It is also possible to directly embed the connection as follows:
```yaml
relations:
  frontend_users:
    kind: sqlserver
    # Specify the name of the connection to use
    connection:
      kind: jdbc
      url: "$frontend_db_url"
      username: "$frontend_db_username"
      password: "$frontend_db_password"
    # Specify the table
    table: "users"
    # Specify storage format
    storageFormat: COLUMNSTORE
```
For most cases, it is recommended not to embed the connection, since this prevents reusing the same connection in
multiple places.


## Fields
 * `kind` **(mandatory)** *(type: string)*: `jdbc`
   
 * `schema` **(optional)** *(type: schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema.

 * `primaryKey`  **(optional)** *(type: list)* *(default: empty)*:
List of columns which form the primary key. This will be used when Flowman creates the table, and this will also be used
as the fallback for merge/upsert operations, when no `mergeKey` and no explicit merge condition is specified.

 * `mergeKey`  **(optional)** *(type: list)* *(default: empty)*:
  List of columns which will be used as default condition for merge and upsert operations. The main difference to
 `primaryKey` is that these columns will not be used as a primary key for creating the table.
 
 * `description` **(optional)** *(type: string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `connection` **(mandatory)** *(type: string)*:
 The *connection* field specifies the name of a [JDBC Connection](../connection/jdbc.md)
 object which has to be defined elsewhere.
 
 * `database` **(optional)** *(type: string)* *(default: empty)*: 
 Defines the Hive database where the table is defined. When no database is specified, the table is accessed without any 
specific qualification, meaning that the default database will be used, or the one specified in the connection.

 * `table` **(mandatory)** *(type: string)*:
 Specifies the name of the table in the relational database.
 
 * `stagingTable` **(optional)** *(type: string)*  *(default: empty)*:
  Specifies the name of the staging table to use for write operations. This table will be created and filled with data,
and the final table will be populated from this table inside a transaction. If not table is specified (the default),
then Flowman will use a global temporary table.

 * `properties` **(optional)** *(type: map:string)* *(default: empty)*:
 Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
 relation and the JDBC connection can define properties. So it is advisable to define all
 common properties in the connection and more table specific properties in the relation.
 The connection properties are applied first, then the relation properties. This means that
 a relation property can overwrite a connection property if it has the same name.

 * `indexes` **(optional)** *(type: list:index)* *(default: empty)*:
 Specifies a list of database indexes to be created. Each index has the properties `name`, `columns`, `unique`
 (default=`false`) and `clustered` (default=`false`). Note that `clustered` indexes are currently only supported by MS
 Flowman for SQL Server and Azure SQL.

 * `storageformat` **(optional)** *(type: string)* *(default: empty)*:
 Specifies the internal storage format, which can either be `ROWSTORE` or `COLUMNSTORE`. Internally MS SQL Server
 uses `ROWSTORE` as the default format. `COLUMNSTORE` will actually create a `CLUSTERED COLUMNSTORE INDEX` and is
 preferable for typical OLAP workloads.

* `migrationPolicy` **(optional)** *(string)* *(default: empty)*
  Can be one of `RELAXED` or `STRICT`. If left empty, then the value of the Flowman configuration property
  `flowman.default.relation.migrationPolicy` will be used instead.

* `migrationStrategy` **(optional)** *(string)* *(default: empty)*
  Can be one of `ALTER`, `ALTER_REPLACE`, `REPLACE`, `NEVER` or `FAIL`. If left empty, then the value of the Flowman
  configuration property `flowman.default.relation.migrationStrategy` will be used instead.


## Staging Tables
When using the `sqlserver?` relation, Flowman will always use staging tables when writing to a SQL database. This
means, Flowman will first create this special staging table (which technically is just a normal table, but without any 
index or primary key), and then copy the table into the real target table. Afterward, the staging table will be dropped.
This approach helps to ensure consistency, since the copy process is performed within a single SQL transaction. Moreover, 
since no primary key or index is present in the staging table, this will also avoid locks on the database server side,
which may lead to timeouts or other failures during the parallel write process that Spark uses under the hood.

You can either explicitly specify the name of the staging table via `stagingTable`, or Flowman will automatically use
a global temporary table. In both cases, Flowman will automatically remove the staging table after the write operation
has finished (either successfully or with an error).


## Automatic Migrations
Flowman supports some [automatic migrations](../../concepts/migrations.md), specifically with the migration strategies 
`ALTER`, `ALTER_REPLACE` and `REPLACE` (those can be set via the property `migrationStrategy` or the global config variable
`flowman.default.relation.migrationStrategy`, see [configuration](../../setup/config.md) for more details).

The migration strategy `ALTER` supports the following alterations for JDBC relations:
* Changing nullability
* Adding new columns
* Dropping columns
* Changing the column type
* Adding / dropping indexes
* Changing the primary key

Note that although Flowman will try to apply these changes, not all SQL databases support all of these changes in
all variations. Therefore, it may well be the case, that the SQL database will fail performing these changes. If
the migration strategy is set to `ALTER_REPLACE`, then Flowman will fall back to trying to replace the whole table
altogether on *any* non-recoverable exception during migration.


## Schema Conversion
The JDBC relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).


## Output Modes
The `sqlserver` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                              |
|---------------------|-----------|-------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the JDBC table already exists       |
| `ignoreIfExists`    | yes       | Do nothing if the JDBC table already exists           |
| `overwrite`         | yes       | Overwrite the whole table or the specified partitions |
| `overwrite_dynamic` | no        | -                                                     |
| `append`            | yes       | Append new records to the existing table              |
| `update`            | yes       | -                                                     |

In addition, the `sqlserver` relation also supports complex merge operations in a [`merge` target](../target/merge.md).


## Remarks

When using SQL Server tables as data sinks in a [`relation` target](../target/relation.md), then Flowman will  manage the
whole lifecycle for you. This means that
* SQL Server tables will be created and migrated during `CREATE` phase, but only if a schema is provided
* SQL Server tables will be populated with records and partitions will be added during `BUILD` phase, but only if the
  `relation` target contains a mapping.
* SQL Server tables will be truncated, or individual partitions will be dropped during `TRUNCATE` phase
* SQL Server tables will be removed during `DESTROY` phase

This means that you can
* Externally manage tables by omitting the schema. Then Flowman will not create or migrate the table for
  any [`relation` target](../target/relation.md) referring to this relation.
* Only manage the tables by Flowman but not populate it with data by omitting a mapping in the
  [`relation` target](../target/relation.md).

### Mocking SQL server relations
Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data 
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema 
in all situations where only schema information is required.

### Using staging tables
Since version 0.23.0 Flowman will always use a global temporary table as a staging table for any write/update
operation for SQL Server relations.

While this two-step approach might slow down write processes, it is often required when performing update/merge
operations since these could result in database deadlocks otherwise when Spark performs these operations in parallel
from multiple processes into a single database.
