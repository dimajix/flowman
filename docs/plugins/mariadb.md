# MariaDB Plugin

The MariaDB plugin mainly provides a JDBC driver to access MariaDB databases via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mariadb 
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
