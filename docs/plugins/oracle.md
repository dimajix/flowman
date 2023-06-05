# Oracle Plugin

The Oracle plugin mainly provides a JDBC driver to access Oracle databases via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-oracle
```


## Usage

In order to connect to an Oracle database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "oracle.jdbc.OracleDriver"
    url: "jdbc:oracle:thin:@my-oracle-server.domain.com/my-database"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify the table
    table: "users"
```


## Data Types
Flowman will map its [built in data types](../spec/fields.md) to the following data types in MariaDB

| Flowman/Spark Datatype | MariaDB Datatype |
|------------------------|------------------|
| `string`, `text`       | `NVARCHAR2(255)` |
| `binary`               | `BLOB`           |
| `tinyint`, `byte`      | `NUMBER(3)`      |
| `smallint`, `short`    | `NUMBER(5)`      |
| `int`, `integer`       | `NUMBER(10)`     |
| `bigint`, `long`       | `NUMBER(19)`     |
| `boolean`, `bool`      | `NUMBER(1)`      |
| `float`                | `BINARY_FLOAT`   |
| `double`               | `BINARY_DOUBLE`  |
| `decimal(a,b)`         | `DECIMAL(a,b)`   |
| `varchar(n)`           | `NVARCHAR2(n)`   |
| `char(n)`              | `NCHAR(n)`       |
| `date`                 | `DATE`           |
| `timestamp`            | `TIMESTAMP`      |
| `duration`             | unsupported      |
