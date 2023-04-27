# Installation Guide

## Requirements

Flowman brings many dependencies with the installation archive, but everything related to Hadoop or Spark needs to 
be provided by your platform. This approach ensures that the existing Spark and Hadoop installation is used together
with all patches and extensions available on your platform. Specifically, this means that Flowman requires the following
components present on your system:

* Java 11 (Java 1.8 for some specific versions)
* Apache Spark with a matching minor version 
* Apache Hadoop with a matching minor version

Note that Flowman can be built for different Hadoop and Spark versions, and the major and minor version of the build
needs to match the ones of your platform


# Downloading Flowman

Currently, since version 0.14.1, prebuilt releases are provided on [GitHub](https://github.com/dimajix/flowman/releases).
This is probably the simplest way to grab a working Flowman package. Note that for each release, there are different
packages being provided, for different Spark and Hadoop versions. The naming is straightforward:
```
flowman-dist-<version>-oss-spark<spark-version>-hadoop<hadoop-version>-bin.tar.gz
```
You simply have to use the package which fits to the Spark and Hadoop versions of your environment. For example, the
package of Flowman 1.0.0 and for Spark 3.3 and Hadoop 3.3 would be
```
flowman-dist-1.0.0-oss-spark3.3-hadoop3.3-bin.tar.gz
```
and the full URL then would be
```
https://github.com/dimajix/flowman/releases/download/1.0.0/flowman-dist-1.0.0-oss-spark3.3-hadoop3.3-bin.tar.gz
```

## Supported Spark Environments
Flowman is available for a large number of different Spark/Hadoop environments. The following variants are available:

| Distribution     | Spark | Hadoop | Java | Scala | Variant                    |
|------------------|-------|--------|------|-------|----------------------------|
| Open Source      | 2.4.8 | 2.6    | 1.8  | 2.11  | oss-spark2.4-hadoop2.6     |
| Open Source      | 2.4.8 | 2.7    | 1.8  | 2.11  | oss-spark2.4-hadoop2.7     |
| Open Source      | 3.0.3 | 2.7    | 11   | 2.12  | oss-spark3.0-hadoop2.7     |
| Open Source      | 3.0.3 | 3.2    | 11   | 2.12  | oss-spark3.0-hadoop3.2     |
| Open Source      | 3.1.2 | 2.7    | 11   | 2.12  | oss-spark3.1-hadoop2.7     |
| Open Source      | 3.1.2 | 3.2    | 11   | 2.12  | oss-spark3.1-hadoop3.2     |
| Open Source      | 3.2.3 | 2.7    | 11   | 2.12  | oss-spark3.2-hadoop2.7     |
| Open Source      | 3.2.3 | 3.3    | 11   | 2.12  | oss-spark3.2-hadoop3.3     |
| Open Source      | 3.3.2 | 2.7    | 11   | 2.12  | oss-spark3.3-hadoop2.7     |
| Open Source      | 3.3.2 | 3.3    | 11   | 2.12  | oss-spark3.3-hadoop3.3     |
| AWS EMR 6.10     | 3.3.1 | 3.3    | 1.8  | 2.12  | emr-spark3.3-hadoop3.3     |
| Azure Synapse    | 3.3.1 | 3.3    | 1.8  | 2.12  | synapse-spark3.3-hadoop3.3 |
| Cloudera CDH 6.3 | 2.4.0 | 3.0    | 1.8  | 2.11  | cdh6-spark2.4-hadoop3.0    |
| Cloudera CDP 7.1 | 2.4.8 | 3.1    | 1.8  | 2.11  | cdp7-spark2.4-hadoop3.1    |
| Cloudera CDP 7.1 | 3.2.1 | 3.1    | 11   | 2.12  | cdp7-spark3.2-hadoop3.1    |
| Cloudera CDP 7.1 | 3.3.0 | 3.1    | 11   | 2.12  | cdp7-spark3.3-hadoop3.1    |


## Building Flowman

As an alternative to downloading a pre-built distribution of Flowman, you might also want to 
[build Flowman](building.md) yourself in order to match your environment. A task which is not difficult for someone who
 has basic  experience with Maven.


# Local Installation

Flowman is distributed as a `tar.gz` file, which simply needs to be extracted at some location on your computer or 
server. This can be done via
```shell script
tar xvzf flowman-dist-X.Y.Z-bin.tar.gz
```

### Directory Layout

```bash
├── bin
├── conf
├── examples
│   ├── plugin-example
│   │   └── job
│   ├── sftp-upload
│   │   ├── config
│   │   ├── data
│   │   └── job
│   └── weather
│       ├── config
│       ├── job
│       ├── mapping
│       ├── model
│       └── target
├── lib
├── libexec
└── plugins
    ├── flowman-aws
    ├── flowman-azure
    ├── flowman-example
    ├── flowman-impala
    ├── flowman-kafka
    └── flowman-mariadb
```

* The `bin` directory contains the Flowman executables
* The `conf` directory contains global configuration files
* The `lib` directory contains all Java jars
* The `libexec` directory contains some internal helper scripts
* The `plugins` directory contains more subdirectories, each containing a single plugin
* The `examples` directory contains some example projects 


# Configuration

You probably need to perform some basic global configuration of Flowman. The relevant files are stored in the `conf`
directory.

### `flowman-env.sh`

The `flowman-env.sh` script sets up an execution environment on a system level. Here some very fundamental Spark
and Hadoop properties can be configured, like for example
* `SPARK_HOME`, `HADOOP_HOME` and related environment variables
* `KRB_PRINCIPAL` and `KRB_KEYTAB` for using Kerberos
* Generic Java options like http proxy and more

#### Example
```shell script
#!/usr/bin/env bash

# Specify Java home (just in case)
#
#export JAVA_HOME

# Explicitly override Flowmans home. These settings are detected automatically,
# but can be overridden
#
#export FLOWMAN_HOME
#export FLOWMAN_CONF_DIR

# Specify any environment settings and paths
#
#export SPARK_HOME
#export HADOOP_HOME
#export HADOOP_CONF_DIR=${HADOOP_CONF_DIR=$HADOOP_HOME/conf}
#export YARN_HOME
#export HDFS_HOME
#export MAPRED_HOME
#export HIVE_HOME
#export HIVE_CONF_DIR=${HIVE_CONF_DIR=$HIVE_HOME/conf}

# Set the Kerberos principal in YARN cluster
#
#KRB_PRINCIPAL=
#KRB_KEYTAB=

# Specify the YARN queue to use
#
#YARN_QUEUE=

# Use a different spark-submit (for example spark2-submit in Cloudera)
#
#SPARK_SUBMIT=


# Apply any proxy settings from the system environment
#
if [[ "$PROXY_HOST" != "" ]]; then
    SPARK_DRIVER_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_DRIVER_JAVA_OPTS"

    SPARK_EXECUTOR_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_EXECUTOR_JAVA_OPTS"

    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.proxy.host=${PROXY_HOST}
        --conf spark.hadoop.fs.s3a.proxy.port=${PROXY_PORT}
        $SPARK_OPTS"
fi

# Set AWS credentials if required. You can also specify these in project config
#
if [[ "$AWS_ACCESS_KEY_ID" != "" ]]; then
    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
        --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
        $SPARK_OPTS"
fi
```

### `system.yml`
After the execution environment has been setup, the `system.yml` is the first configuration file processed by the Java
application. Its main purpose is to load some fundamental plugins, which are already required by the next level of 
configuration 

#### Example
```yaml
plugins:
  - flowman-impala
```

### `default-namespace.yml`
On top of the very global settings, Flowman also supports so called *namespaces*. Each project is executed within the
context of one namespace, if nothing else is specified the *defautlt namespace*. Each namespace contains some 
configuration, such that different namespaces might represent different tenants or different staging environments.

#### Example
```yaml
name: "default"

history:
  kind: jdbc
  connection: flowman_state
  retries: 3
  timeout: 1000

hooks:
  - kind: web
    jobSuccess: http://some-host.in.your.net/success&job=$URL.encode($job)&force=$force

connections:
  flowman_state:
    driver: $System.getenv('FLOWMAN_HISTORY_DRIVER', 'org.apache.derby.jdbc.EmbeddedDriver')
    url: $System.getenv('FLOWMAN_HISTORY_URL', $String.concat('jdbc:derby:', $System.getenv('FLOWMAN_HOME'), '/logdb;create=true'))
    username: $System.getenv('FLOWMAN_HISTORY_USER', '')
    password: $System.getenv('FLOWMAN_HISTORY_PASSWORD', '')

config:
  - spark.sql.warehouse.dir=$System.getenv('FLOWMAN_HOME')/hive/warehouse
  - hive.metastore.uris=
  - javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$System.getenv('FLOWMAN_HOME')/hive/db;create=true
  - datanucleus.rdbms.datastoreAdapterClassName=org.datanucleus.store.rdbms.adapter.DerbyAdapter

plugins:
  - flowman-delta
  - flowman-aws
  - flowman-azure
  - flowman-kafka
  - flowman-mariadb

store:
  kind: file
  location: $System.getenv('FLOWMAN_HOME')/examples
```


## Running in a Kerberized Environment
Please have a look at [Kerberos](cookbook/kerberos.md) for detailed information.

## Deploying with Docker
It is also possible to run Flowman inside Docker. This simply requires a Docker image with a working Spark and
Hadoop installation such that Flowman can be installed inside the image just as it is installed locally.
