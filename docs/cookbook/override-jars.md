# Override jar versions

A common problem with Spark and specifically with many Hadoop environments (like Cloudera) are mismatches between
application jar versions and jars provided by the runtime environment. Flowman is built with carefully set dependency
version to match those of each supported runtime environment. But sometimes this might not be enough.

For example Cloudera ships with a rather old JDBC driver for MS SQL Server / Azure SQL Server which is not compatible
with the `sqlserver` relation type provided by the [MS SQL Server plugin](../plugins/mssqlserver.md). This will result
in `ClassNotFound` or `MethodNotFound` exceptions during execution. But it is still
possible to force Spark to use the newer JDBC driver by changing some config options.


## Configuration

You need to add the following lines to your custom `flowman-env.sh` file which is stored in the `conf` subdirectory:

```shell
# Add MS SQL JDBC Driver. Normally this is handled by the plugin mechanism, but Cloudera already provides some
# old version of the JDBC driver, and this is the only place where we can force to use our JDBC driver
SPARK_JARS="$FLOWMAN_HOME/plugins/flowman-mssqlserver/mssql-jdbc-9.2.1.jre8.jar"
SPARK_OPTS="--conf spark.executor.extraClassPath=mssql-jdbc-9.2.1.jre8.jar --conf spark.driver.extraClassPath=$FLOWMAN_HOME/plugins/flowman-mssqlserver/mssql-jdbc-9.2.1.jre8.jar"
```
The first line will explicitly add the plugin jar to the list of jars as passed to `spark-submit`. But this is still
not enough, we also have to set `spark.executor.extraClassPath` and `spark.driver.extraClassPath` which will *prepend* the specified jars to the
classpath of the executor.
