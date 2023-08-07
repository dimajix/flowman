# Trino Plugin

The Trino plugin mainly provides a JDBC driver to access Trino via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-trino 
```

## Usage

In order to connect to a Trino cluster, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  my_trino_cluster:
    kind: jdbc
    driver: "io.trino.jdbc.TrinoDriver"
    url: "jdbc:trino://my-trino-cluster.domain.com/my_default_catalog"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: my_trino_cluster
    # Specify database
    database: "frontend"
    # Specify the table
    table: "users"
```


## Data Types
Flowman will map its [built in data types](../spec/fields.md) to the following data types in MariaDB

| Flowman/Spark Datatype | Trino Datatype |
|------------------------|----------------|
| `string`, `text`       | `VARCHAR`      |
| `binary`               | `VARBINARY`    |
| `tinyint`, `byte`      | `TINYINT`      |
| `smallint`, `short`    | `SMALLINT`     |
| `int`, `integer`       | `INTEGER`      |
| `bigint`, `long`       | `BIGINT`       |
| `boolean`, `bool`      | `BOOLEAN`      |
| `float`                | `REAL`         |
| `double`               | `DOUBLE`       |
| `decimal(a,b)`         | `DECIMAL(a,b)` |
| `varchar(n)`           | `VARCHAR(n)`   |
| `char(n)`              | `CHAR(n)`      |
| `date`                 | `DATE`         |
| `timestamp`            | `TIMESTAMP`    |
| `duration`             | unsupported    |
