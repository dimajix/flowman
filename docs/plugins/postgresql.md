# PostgreSQL Plugin

The PostgreSQL plugin mainly provides a JDBC driver to access PostgreSQL databases via the [JDBC relation](../spec/relation/jdbcTable.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-postgresql 
```


## Usage

In order to connect to a PostgreSQL database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "org.postgresql.Driver"
    url: "jdbc:postgresql://my-postgres-database.domain.com/my_database"
    username: "my_username"
    password: "secret!password"

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify schema
    database: "frontend"
    # Specify the table
    table: "users"
```


## Data Types
Flowman will map its [built in data types](../spec/fields.md) to the following data types in MariaDB

| Flowman/Spark Datatype | MariaDB Datatype |
|------------------------|------------------|
| `string`, `text`       | `TEXT`           |
| `binary`               | `BYTEA`          |
| `tinyint`, `byte`      | `BYTE`           |
| `smallint`, `short`    | `SMALLINT`       |
| `int`, `integer`       | `INTEGER`        |
| `bigint`, `long`       | `BIGINT`         |
| `boolean`, `bool`      | `BOOLEAN`        |
| `float`                | `FLOAT4`         |
| `double`               | `FLOAT8`         |
| `decimal(a,b)`         | `NUMERIC(a,b)`   |
| `varchar(n)`           | `VARCHAR(n)`     |
| `char(n)`              | `CHAR(n)`        |
| `date`                 | `DATE`           |
| `timestamp`            | `TIMESTAMP`      |
| `duration`             | unsupported      |
