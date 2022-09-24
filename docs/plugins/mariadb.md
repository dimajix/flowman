# MariaDB Plugin

The MariaDB plugin mainly provides a JDBC driver to access MariaDB databases via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mariadb 
```

### Fixing SQL syntax errors during write operations
Newer MariaDB versions expect a slightly different syntax for quoting column names as opposed to the way that Spark
is doing it. This can be changing the *SQL mode* on the MariaDB server to allow *ANSI Quotes*:
```sql
SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES';
```
You can also set the *SQL mode* as a MariaDB server startup parameter by adding the following option to the MariaDB daemon:
```shell
--sql-mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES
```


## Usage

In order to connect to a MariaDB database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "org.mariadb.jdbc.Driver"
    url: "jdbc:mariadb://my-mariadb-database.domain.com"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify database
    database: "frontend"
    # Specify the table
    table: "users"
```
