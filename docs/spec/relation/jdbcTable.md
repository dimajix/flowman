# JDBC Table Relations

The `jdbcTable` relation allows you to access databases using a JDBC driver. Note that you need to put an appropriate JDBC
driver onto the classpath of Flowman. This can be done by using an appropriate plugin.


## Example

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "$frontend_db_driver"
    url: "$frontend_db_url"
    username: "$frontend_db_username"
    password: "$frontend_db_password"

relations:
  frontend_users:
    kind: jdbcTable
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
    kind: jdbcTable
    # Specify the name of the connection to use
    connection:
      kind: jdbc
      driver: "$frontend_db_driver"
      url: "$frontend_db_url"
      username: "$frontend_db_username"
      password: "$frontend_db_password"
    # Specify the table
    table: "users"
```
For most cases, it is recommended not to embed the connection, since this prevents reusing the same connection in
multiple places.

It is also possible to access the results of an arbitrary SQL query, which is executed inside the target database:
```yaml
relations:
  lineitem:
    kind: jdbc
    connection: frontend
    query: "
      SELECT
        CONCAT('DIR_', li.id) AS lineitem,
        li.campaign_id AS campaign,
        IF(c.demand_type_system = 1, 'S', IF(li.demand_type_system = 1, 'S', 'D')) AS demand_type
      FROM
        line_item AS li
      INNER JOIN
        campaign c
        ON c.id = li.campaign_id
    "
    schema:
      kind: embedded
      fields:
        - name: lineitem
          type: string
        - name: campaign
          type: long
        - name: demand_type
          type: string
```
The schema is still optional in this case, but it will help [mocking](mock.md) the relation for unittests.


## Fields
 * `kind` **(mandatory)** *(type: string)*: `jdbcTable` or `jdbc`
   
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
 The *connection* field specifies the name of a [Connection](../connection/index.md)
 object which has to be defined elsewhere.
 
 * `database` **(optional)** *(type: string)* *(default: empty)*: 
 Defines the Hive database where the table is defined. When no database is specified, the
 table is accessed without any specific qualification, meaning that the default database
 will be used or the one specified in the connection.

 * `table` **(optional)** *(type: string)*:
 Specifies the name of the table in the relational database. You either need to specify this `table` property
or the `query` property.

 * `query` **(optional)** *(type: string)*:
As an alternative to directly accessing a table, you can also specify an SQL query which will be executed by the
database for retrieving data. Of course, then only read operations are possible. You either need to specify this 
`query` property or the `table` property.
  
 * `properties` **(optional)** *(type: map:string)* *(default: empty)*:
 Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
 relation and the JDBC connection can define properties. So it is advisable to define all
 common properties in the connection and more table specific properties in the relation.
 The connection properties are applied first, then the relation properties. This means that
 a relation property can overwrite a connection property if it has the same name.

 * `indexes` **(optional)** *(type: list:index)* *(default: empty)*:
   Specifies a list of database indexes to be created. Each index has the properties `name`, `columns` and `unique`.


## Automatic Migrations
Flowman supports some automatic migrations, specifically with the migration strategies `ALTER`, `ALTER_REPLACE`
and `REPLACE` (those can be set via the global config variable `flowman.default.relation.migrationStrategy`,
see [configuration](../../config.md) for more details).

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
The `jdbcTable` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                                     |
|---------------------|-----------|--------------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the JDBC table already exists              |
| `ignoreIfExists`    | yes       | Do nothing if the JDBC table already exists                  |
| `overwrite`         | yes       | Overwrite the whole table or the specified partitions        |
| `overwrite_dynamic` | no        | -                                                            |
| `append`            | yes       | Append new records to the existing table                     |
| `update`            | yes       | Perform upsert operations using the merge key or primary key |

In addition, the `jdbcTable` relation also supports complex merge operations in a [`merge` target](../target/merge.md).


## Remarks

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data 
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema 
in all situations where only schema information is required.
