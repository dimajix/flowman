# MS SQL Server Plugin

The MS SQL Server plugin provides a JDBC driver  to access MS SQL Server and Azure SQL Server databases via 
the [JDBC relation](../spec/relation/jdbcTable.md). Moreover, it also provides a specialized  
[`sqlserver` relation](../spec/relation/sqlserver.md) which uses bulk copy to speed up writing process, and it
also uses temp tables to encapsulate the whole data upload within a transaction.


## Provided Entities
* [`sqlserver` relation](../spec/relation/sqlserver.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mssqlserver 
```

### Fixing JDBC driver version issues
Some Hadoop distributions (e.g. Cloudera) come along with an outdated MS SQL JDBC connector. This causes problems with
the plugin. Fortunately you can [manually explicitly force Spark to use the correct JDBC](../cookbook/override-jars.md).
You need to add the following lines to your custom `flowman-env.sh` file which is stored in the `conf` subdirectory:

```shell
# Add MS SQL JDBC Driver. Normally this is handled by the plugin mechanism, but Cloudera already provides some
# old version of the JDBC driver, and this is the only place where we can force to use our JDBC driver
SPARK_JARS="$FLOWMAN_HOME/plugins/flowman-mssqlserver/mssql-jdbc-9.2.1.jre8.jar"
SPARK_OPTS="--conf spark.executor.extraClassPath=mssql-jdbc-9.2.1.jre8.jar --conf spark.driver.extraClassPath=$FLOWMAN_HOME/plugins/flowman-mssqlserver/mssql-jdbc-9.2.1.jre8.jar"
```


## Usage

In order to connect to an MS SQL Server or Azure SQL database, you need to specify a [JDBC connection](../spec/connection/jdbc.md)
and use that one in a [JDBC relation](../spec/relation/jdbcTable.md) as follows:

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url: "jdbc:sqlserver://my-azure-database.domain.com"
    username: "my_username"
    password: "secret!password"
    properties:
      # We need to specify the database name already as part of connection, otherwise the login won't work
      databaseName: my_database

relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify schema (within the database)
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
    url: "jdbc:sqlserver://my-azure-database.domain.com"
    username: "my_username"
    password: "secret!password"
    properties:
        # We need to specify the database name already as part of connection, otherwise the login won't work
        databaseName: my_database

relations:
  frontend_users:
    kind: sqlserver
    # Specify the name of the connection to use
    connection: frontend
    # Specify schema (within the database)
    database: "dbo"
    # Specify the table
    table: "users"
```


## Data Types
Flowman will map its [built in data types](../spec/fields.md) to the following data types in MariaDB

| Flowman/Spark Datatype | MariaDB Datatype   |
|------------------------|--------------------|
| `string`, `text`       | `NVARCHAR(MAX)`    |
| `binary`               | `VARBINARY(MAX)`   |
| `tinyint`, `byte`      | `BYTE`             |
| `smallint`, `short`    | `SMALLINT`         |
| `int`, `integer`       | `INTEGER`          |
| `bigint`, `long`       | `BIGINT`           |
| `boolean`, `bool`      | `BIT`              |
| `float`                | `FLOAT`            |
| `double`               | `DOUBLE PRECISION` |
| `decimal(a,b)`         | `DECIMAL(a,b)`     |
| `varchar(n)`           | `NVARCHAR(n)`      |
| `char(n)`              | `NCHAR(n)`         |
| `date`                 | `DATE`             |
| `timestamp`            | `DATETIME2`        |
| `duration`             | unsupported        |
