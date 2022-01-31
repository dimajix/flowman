# SQL Server Relations

The SQL Server relation allows you to access MS SQL Server and Azure SQL databases using a JDBC driver. It uses the
`spark-sql-connector` from Microsoft to speed up processing.


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
specific qualification, meaning that the default database will be used or the one specified in the connection.

 * `table` **(mandatory)** *(type: string)*:
 Specifies the name of the table in the relational database.
  
 * `properties` **(optional)** *(type: map:string)* *(default: empty)*:
 Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
 relation and the JDBC connection can define properties. So it is advisable to define all
 common properties in the connection and more table specific properties in the relation.
 The connection properties are applied first, then the relation properties. This means that
 a relation property can overwrite a connection property if it has the same name.


## Automatic Migrations
Flowman supports some automatic migrations, specifically with the migration strategies `ALTER`, `ALTER_REPLACE`
and `REPLACE` (those can be set via the global config variable `flowman.default.relation.migrationStrategy`,
see [configuration](../../config.md) for more details).

The migration strategy `ALTER` supports the following alterations for JDBC relations:
* Changing nullability
* Adding new columns
* Dropping columns
* Changing the column type

Note that although Flowman will try to apply these changes, not all SQL databases support all of these changes in
all variations. Therefore it may well be the case, that the SQL database will fail performing these changes. If
the migration strategy is set to `ALTER_REPLACE`, then Flowman will fall back to trying to replace the whole table
altogether on *any* non-recoverable exception during migration.


## Schema Conversion
The JDBC relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).


## Output Modes
The `jdbc` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                              |
|---------------------|-----------|-------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the JDBC table already exists       |
| `ignoreIfExists`    | yes       | Do nothing if the JDBC table already exists           |
| `overwrite`         | yes       | Overwrite the whole table or the specified partitions |
| `overwrite_dynamic` | no        | -                                                     |
| `append`            | yes       | Append new records to the existing table              |
| `update`            | no        | -                                                     |
| `merge`             | no        | -                                                     |


## Remarks

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data 
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema 
in all situations where only schema information is required.
