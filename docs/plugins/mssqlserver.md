# MS SQL Server Plugin

The MS SQL Server plugin provides a JDBC driver  to access MS SQL Server and Azure SQL Server databases via 
the [JDBC relation](../spec/relation/jdbcTable.md). Moreover, it also provides a specialized  
[`sqlserver` relation](../spec/relation/sqlserver.md) which uses bulk copy to speed up writing process and it
also uses temp tables to encapsulate the whole data upload within a transaction.


## Provided Entities
* [`sqlserver` relation](../spec/relation/sqlserver.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mssqlserver 
```


## Usage

In order to connect to a MS SQL Server or Azure SQL database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url: "jdbc:sqlserver:my-postgres-database.domain.com/my_database"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify schema
    database: "dbo"
    # Specify the table
    table: "users"
```

In order to benefit from faster write operations, you should use the [`sqlserver` relation](../spec/relation/sqlserver.md)
instead of the generic `jdbc` relation:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    url: "jdbc:sqlserver:my-postgres-database.domain.com/my_database"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: sqlserver
    # Specify the name of the connection to use
    connection: frontend
    # Specify schema
    database: "dbo"
    # Specify the table
    table: "users"
```
