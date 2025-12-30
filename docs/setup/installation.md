# Installation Guide

This setup guide will walk you to an installation of Apache Spark and Flowman on your local Linux box. If you
are using Windows, you will find some hints for setting up the required "Hadoop Winutils", but we generally recommend
to use Linux. You can also run a [Flowman Docker image](docker.md), which is the simplest way to get up to speed.


## 1. Requirements

Flowman brings many dependencies with the installation archive, but everything related to Hadoop or Spark needs to 
be provided by your platform. This approach ensures that the existing Spark and Hadoop installation is used together
with all patches and extensions available on your platform. Specifically this means that Flowman requires the following
components present on your system:

* Java 11 (or 1.8 when using Spark 2.4)
* Apache Spark with a matching minor version 
* Apache Hadoop with a matching minor version

Note that Flowman can be built for different Hadoop and Spark versions, and the major and minor version of the build
needs to match the ones of your platform

### Download & Install Spark

As of this writing, the latest release of Flowman is 1.2.0 and is available prebuilt for Spark 3.4.1 on the Spark
homepage. So we download the appropriate Spark distribution from the Apache archive and unpack it.

```shell
# Create a nice playground which doesn't mess up your system
mkdir playground
cd playground

# Download and unpack Spark & Hadoop
curl -L https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz | tar xvzf -# Create a nice link
ln -snf spark-3.4.1-bin-hadoop3 spark
```
The Spark package already contains Hadoop, so with this single download you already have both installed and integrated with each other.

Once you have installed Spark, you should set the environment variable `SPARK_HOME`, so Flowman can find it
```shell
export SPARK_HOME=<your-spark-installation-directory>
```
It might be a good idea to add a corresponding line to your `.bashrc` or `.profile`.

### Download & Install Hadoop Utils for Windows

If you are trying to run Flowman on Windows, you also need the *Hadoop Winutils*, which is a set of
DLLs required for the Hadoop libraries to be working. You can get a copy at https://github.com/kontext-tech/winutils.
Once you downloaded the appropriate version, you need to place the DLLs into a directory `$HADOOP_HOME/bin`, where
`HADOOP_HOME` refers to some arbitrary location of your choice on your Windows PC. You also need to set the following
environment variables:
* `HADOOP_HOME` should point to the parent directory of the `bin` directory
* `PATH` should also contain `$HADOOP_HOME/bin`

The documentation contains a [dedicated section for Windows users](windows.md)


## 2. Downloading Flowman

Since version 0.14.1, prebuilt releases are provided on the [Flowman Homepage](https://flowman.io) or on 
[GitHub](https://github.com/dimajix/flowman/releases). This probably is the simplest way to grab a working Flowman 
package. Note that for each release, there are different packages being provided, for different Spark and Hadoop 
versions. The naming is straight forward:
```
flowman-dist-<version>-oss-spark<spark-version>-hadoop<hadoop-version>-bin.tar.gz
```
You simply have to use the package which fits to the Spark and Hadoop versions of your environment. For example, the
package of Flowman 1.2.0 and for Spark 3.4 and Hadoop 3.3 would be
```
flowman-dist-1.2.0-oss-spark3.4-hadoop3.3-bin.tar.gz
```
and the full URL then would be
```
https://github.com/dimajix/flowman/releases/download/1.2.0/flowman-dist-1.2.0-oss-spark3.4-hadoop3.3-bin.tar.gz
```

### Supported Spark Environments
Flowman is available for many different Spark/Hadoop environments. The following variants are available:

| Distribution     | Spark | Hadoop | Java | Scala | Variant                       |
|------------------|-------|--------|------|-------|-------------------------------|
| Open Source      | 3.0.3 | 3.2    | 11   | 2.12  | oss-spark3.0-hadoop3.2        |
| Open Source      | 3.1.2 | 3.2    | 11   | 2.12  | oss-spark3.1-hadoop3.2        |
| Open Source      | 3.2.4 | 3.3    | 11   | 2.12  | oss-spark3.2-hadoop3.3        |
| Open Source      | 3.3.3 | 3.3    | 11   | 2.12  | oss-spark3.3-hadoop3.3        |
| Open Source      | 3.4.1 | 3.3    | 11   | 2.12  | oss-spark3.4-hadoop3.3        |
| Open Source      | 3.5.1 | 3.3    | 11   | 2.12  | oss-spark3.5-hadoop3.3        |
| AWS EMR 6.12     | 3.4.0 | 3.3    | 1.8  | 2.12  | emr6.12-spark3.4-hadoop3.3    |
| Azure Synapse    | 3.3.1 | 3.3    | 1.8  | 2.12  | synapse3.3-spark3.3-hadoop3.3 |
| Cloudera CDP 7.1 | 3.2.1 | 3.1    | 11   | 2.12  | cdp7-spark3.2-hadoop3.1       |
| Cloudera CDP 7.1 | 3.3.0 | 3.1    | 11   | 2.12  | cdp7-spark3.3-hadoop3.1       |


### Building Flowman

As an alternative to downloading a pre-built distribution of Flowman, you might also want to 
[build Flowman](building.md) yourself in order to match your environment. A task which is not difficult for someone who
 has basic  experience with Maven.


## 3. Local Installation

Flowman is distributed as a `tar.gz` file, which simply needs to be extracted at some location on your computer or 
server. This can be done via
```shell
tar xvzf flowman-dist-X.Y.Z-bin.tar.gz
```

### Directory Layout
Once you downloaded and unpacked Flowman, you will get a new directory which looks as follows:
```bash
├── bin
├── conf
├── examples
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
├── yaml-schema
└── plugins
    ├── flowman-aws
    ├── flowman-azure
    ├── flowman-impala
    ├── flowman-kafka
    ├── flowman-mariadb
    └── flowman-...
```

* The `bin` directory contains the Flowman executables
* The `conf` directory contains global configuration files
* The `lib` directory contains all Java jars
* The `libexec` directory contains some internal helper scripts
* The `plugins` directory contains more subdirectories, each containing a single plugin
* The `yaml-schema` directory contains [YAML schema files for syntax highlighting and auto-completion](../cookbook/syntax-highlighting.md) 
inside the code editor of your choice.
* The `examples` directory contains some example projects 


## 4. Install additional Hadoop dependencies

Starting with version 3.2, Spark has reduced the number of Hadoop libraries which are part of the downloadable Spark
distribution. Unfortunately, some of the libraries which have been removed are required by some Flowman plugins (for 
example the S3 and Delta plugin need the `hadoop-commons` library). Since at the same time Flowman will for good 
reasons not include these missing libraries, you have to install these yourself and put them into the 
`$SPARK_HOME/jars` folder.

In order to simplify getting the appropriate Hadoop libraries and placing them into the correct Spark directory,
Flowman provides a small script called `install-hadoop-dependencies`, which will download and install the missing
jars:

```shell
export SPARK_HOME=your-spark-home

cd $FLOWMAN_HOME
bin/install-hadoop-dependencies
```

Note that you need to have appropriate write permissions into the `$SPARK_HOME/jars` directory, so you possibly need
to execute this with super-user privileges.


## 5. Configuration (optional)

You probably need to perform some basic global configuration of Flowman. The relevant files are stored in the `conf`
directory.

### `flowman-env.sh`

The `flowman-env.sh` script sets up an execution environment on a system level. Here some very fundamental Spark
and Hadoop properties can be configured, like for example
* `SPARK_HOME`, `HADOOP_HOME` and related environment variables
* `KRB_PRINCIPAL` and `KRB_KEYTAB` for using Kerberos
* Generic Java options like HTTP proxy and more

#### Example
```shell
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
After the execution environment has been set up, the `system.yml` is the first configuration file processed by the Java
application. Its main purpose is to load some fundamental plugins, which are already required by the next level of 
configuration 

#### Example
```yaml
plugins:
  - flowman-impala
```

### `default-namespace.yml`
On top of the very global settings, Flowman also supports so-called *namespaces*. Each project is executed within the
context of one namespace, which would be the *default namespace* if nothing else is specified. Each namespace contains 
some configuration, such that different namespaces might represent different tenants or different staging environments.

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
  - flowman-aws
  - flowman-azure
  - flowman-kafka
  - flowman-mariadb

store:
  kind: file
  location: $System.getenv('FLOWMAN_HOME')/examples
```

### `log4j.properties`
In order to control the console (logging) output at a very detailed level, you can provide your own version of a 
Log4j configuration file inside the `conf` directory. You will find templates both for Log4j 1.x and 2.x.


## 6. Running Flowman

Now when you have installed Spark and Flowman, you can easily start Flowman via
```shell
cd <your-flowman-directory>
export SPARK_HOME=<our-spark-directory>

bin/flowshell -f examples/weather
```


## 7. Related Topics

### Running Flowman on Windows
Please have a look at [Running Flowman on Windows](windows.md) for detailed information.


### Running in a Kerberized Environment
Please have a look at [Kerberos](../cookbook/kerberos.md) for detailed information.


###  Running in Docker
It is also possible to [run Flowman inside Docker](docker.md), which already offers a lot of functionality on developers
local machines. Official Docker images are provided at [Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman)
