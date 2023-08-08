# JDBC Connection

## Example
```yaml
environment:
  - mysql_db_driver=$System.getenv('MYSQL_DRIVER', 'com.mysql.cj.jdbc.Driver')
  - mysql_db_url=$System.getenv('MYSQL_URL')
  - mysql_db_username=$System.getenv('MYSQL_USERNAME')
  - mysql_db_password=$System.getenv('MYSQL_PASSWORD')

connections:
  mysql-db:
    kind: jdbc
    driver: "$mysql_db_driver"
    url: "$mysql_db_url"
    username: "$mysql_db_username"
    password: "$mysql_db_password"
```

## Fields
* `kind` **(mandatory)** *(string)*: `jdbc`

* `driver` **(mandatory)** *(string)*
The Java class of the driver. Common drivers are:
  * MySQL: `com.mysql.cj.jdbc.Driver`
  * MariaDB: `org.mariadb.jdbc.Driver`
  * SQL Server: `com.microsoft.sqlserver.jdbc.SQLServerDriver`
  * Trino: `io.trino.jdbc.TrinoDriver`
  * PostgreSQL: `org.postgresql.Driver`
  * Oracle: `oracle.jdbc.OracleDriver`

* `url` **(mandatory)** *(string)*
The URL of the database. Typical something like `jdbc:postgresql://my-postgres-database.domain.com/my_database`

* `username` **(optional)** *(string)*
The username to log in

* `password` **(optional)** *(string)*
The password to log in 

* `properties` **(optional)** *(map)* 
Additional, database-specific properties. These are passed to the JDBC driver.


## Description
The `jdbc` connection is used to store all connection and credential information along with properties used for 
connecting to a JDBC database. This connection then can be used in the [`jdbcTable`](../relation/jdbcTable.md),
[`jdbcView`](../relation/jdbcView.md) and [`jdbcQuery`](../relation/jdbcQuery.md) relation types.


## JDBC Drivers
You also need to provide an appropriate JDBC driver. These are commonly available as Flowman plugins, for example
* [MariaDB Plugin](../../plugins/mariadb.md)
* [MySQL Plugin](../../plugins/mysql.md)
* [Oracle Plugin](../../plugins/oracle.md)
* [Postgres Plugin](../../plugins/postgresql.md)
* [SQL Server Plugin](../../plugins/mssqlserver.md)
* [Trino Plugin](../../plugins/trino.md)
