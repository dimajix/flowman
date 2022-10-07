# MySQL Plugin

The MySQL plugin mainly provides a JDBC driver to access MySQL databases via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mysql 
```


## Usage

In order to connect to a MySQL database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "com.mysql.cj.jdbc.Driver"
    url: "jdbc:mysql://my-mysql-database.domain.com"
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


## Data Types
Flowman will map its [built in data types](../spec/fields.md) to the following data types in MariaDB

| Flowman/Spark Datatype | MariaDB Datatype   |
|------------------------|--------------------|
| `string`, `text`       | `TEXT`             |
| `binary`               | `BLOB`             |
| `tinyint`, `byte`      | `BYTE`             |
| `smallint`, `short`    | `INTEGER`          |
| `int`, `integer`       | `INTEGER`          |
| `bigint`, `long`       | `BIGINT`           |
| `boolean`, `bool`      | `BIT(1)`           |
| `float`                | `FLOAT`            |
| `double`               | `DOUBLE PRECISION` |
| `decimal(a,b)`         | `DECIMAL(a,b)`     |
| `varchar(n)`           | `VARCHAR(n)`       |
| `char(n)`              | `CHAR(n)`          |
| `date`                 | `DATE`             |
| `timestamp`            | `TIMESTAMP`        |
| `duration`             | unsupported        |
