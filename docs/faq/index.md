# FAQ

You are running into some trouble? Look here, or get in touch with the [creators of Flowman](mailto:info@flowman.io).


## 1. Installation

### `ClassNotFoundException` when reading/writing from/to S3
You have probably downloaded and installed Apache Spark directly from its homepage. Unfortunately since version 3.2, 
some Hadoop libraries are now missing from the official Spark Hadoop distributions. But Flowman contains a [small
script to install the missing dependencies](../cookbook/hadoop-dependencies.md), which should fix the error.


### Wrong jar dependencies used
Some Hadoop/Spark distributions come along with a lot of jars, for example some (outdated) MS SQL JDBC connector. This
can cause problems, but you can [manually explicitly force Spark to use the correct jar](../cookbook/override-jars.md)


## 2. Execution

### SQL error while trying to write to a MariaDB database
There seems to be a change in the default behavior of MariaDB in regard to quoting column names. Spark uses
quotation  marks (`"`), which is not supported by MariaDB in default setup. Fortunately you can change the allowed
*SQL mode* on MariaDB side by executing (with admin privileges) to allow ANSI quotes:
```sql
SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES';
```
You can also set the *SQL mode* as a MariaDB server startup parameter by adding the following option to the MariaDB daemon:
```shell
--sql-mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES
```


### Java error while trying to write to Azure SQL / SQL Server
Some Hadoop distributions (e.g. Cloudera) come along with an outdated MS SQL JDBC connector. This causes problems with
the [MS SQL Server Plugin](../plugins/mssqlserver.md), but  
you can [manually explicitly force Spark to use the correct JDBC](../cookbook/override-jars.md)


### Database locking error while trying to write into a SQL database
You might run into some database locking issues when writing into a [`jdbcTable` relation](../spec/relation/jdbcTable.md).
The reason is that Spark (and therefore Flowman) will perform a highly parallelized write process using multiple threads, 
processes and workers all writing to the same database at the same time. This may be simply too much for the target
database, especially if there are constraints (like a primary key) or indexes. In this case the write operation may 
fail with some error messages complaining about locks. 

In order to mitigate this problem, you can easily instruct Flowman to use a temporary staging table to write to in
a first step and then copy its contents to the final table. This will solve locking issues in most cases, and
can be simply achieved by specifying the (optional) field `stagingTable` as in the following example:

```yaml
relations:
  frontend_users:
    kind: jdbcTable
    # Specify the name of the connection to use
    connection: frontend
    # Specify the table
    table: "users"
    # Specify name of temporary staging table (optional)
    stagingTable: "users_staging"
    schema:
      kind: avro
      file: "${project.basedir}/schema/users.avsc"
    primaryKey:
      - user_id
    indexes:
      - name: "users_idx0"
        columns: [user_first_name, user_last_name]
```


### Changing log level
Flowman provides two methods for changing the log level, either via command line arguments or via a log4j configuration
file. If you temporarily want to increase the logging verbosity, you can simply use the `-X` and `-XX` command line
switches for both [`flowexec`](../cli/flowexec/index.md) and the [Flowman Shell](../cli/flowshell/index.md).

Another option is to provide either a `log4j.properties` or `log4j2.properties` file in the `conf` directory of your
Flowman installation. Note that `log4j2.properties` uses the Log4j 2.x syntax and is only supported since Spark 3.3.
Environments using older Spark versions still have to rely on the Log4j 1.x configuration file `log4j.properties`.
You will find templates for both Log4j versions in the `conf` directory.
