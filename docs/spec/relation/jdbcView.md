# JDBC View Relations

The `jdbcView` relation allows you to access and manage VIEWs in relational databases using a JDBC driver. Note that 
you need to put an appropriate JDBC driver onto the classpath of Flowman. This can be done by using an appropriate 
plugin.

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
    kind: jdbcView
    # Specify the name of the connection to use
    connection: frontend
    # Specify the view name
    view: "users"
    # Specify the SQL statement for defining the VIEW
    sql: |
      SELECT
        * 
      FROM crm.persons
      WHERE permission = 'frontend'
```
It is also possible to directly embed the connection as follows:
```yaml
relations:
  frontend_users:
    kind: jdbcView
    # Specify the name of the connection to use
    connection:
      kind: jdbc
      driver: "$frontend_db_driver"
      url: "$frontend_db_url"
      username: "$frontend_db_username"
      password: "$frontend_db_password"
    # Specify the view name
    view: "users"
    # Specify a file containing the SQL statement
    file: "${project.basedir}/views/frontend_users.sql"
```
For most cases, it is recommended not to embed the connection, since this prevents reusing the same connection in
multiple places.


## Fields
* `kind` **(mandatory)** *(type: string)*: `jdbcView`

* `connection` **(mandatory)** *(type: string)*:
  The *connection* field specifies the name of a [Connection](../connection/index.md)
  object which has to be defined elsewhere.

* `database` **(optional)** *(type: string)* *(default: empty)*:
  Defines the Hive database where the table is defined. When no database is specified, the
  table is accessed without any specific qualification, meaning that the default database
  will be used or the one specified in the connection.

* `view` **(optional)** *(type: string)*:
  Specifies the name of the view in the relational database.

* `sql` **(optional)** *(type: string)*:
  Specifies the SQL query which defines the view.

* `file` **(optional)** *(type: string)*:
  Specifies the name of an external file which contains the actual view definition.

* `properties` **(optional)** *(type: map:string)* *(default: empty)*:
  Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
  relation and the JDBC connection can define properties. So it is advisable to define all
  common properties in the connection and more table specific properties in the relation.
  The connection properties are applied first, then the relation properties. This means that
  a relation property can overwrite a connection property if it has the same name.

Note that either `sql` or `file` needs to be specified. If you want to access an existing view without managing it in
Flowman, then simply use a [`jdbcTable` relation](jdbcTable.md) instead.


## Automatic Migrations
Flowman supports some automatic migrations, specifically with the migration strategies `ALTER`, `ALTER_REPLACE`
and `REPLACE` (those can be set via the global config variable `flowman.default.relation.migrationStrategy`,
see [configuration](../../setup/config.md) for more details).

The migration strategy `ALTER`, `ALTER_REPLACE` and `REPLACE` supports the following alterations for JDBC relations:
* Migrating from a TABLE to a VIEW
* Changing the view definition


## Remarks

Not all databases support retrieving the original SQL definition of a view. In this case it is impossible for Flowman
to check if the view definition is different from the users specification, and will always migrate the view.
