# Deploying Flowman to AWS EMR

Since version 0.30.0, Flowman officially supports AWS EMR (Elastic Map Reduce) as an execution environment. In order
to provide a high degree of compatibility with EMR, Flowman provides special build variants for AWS, which can be
identified via the term "emr" in their version.

For example, the Flowman version `1.0.0-emr6.10-spark3.3-hadoop3.3` contains a build specifically crafted for EMR
version 6.9, which contains Spark 3.3 and Hadoop 3.3. You should always use a Flowman version which matches your
EMR version (or the other way round) in order to avoid incompatibilities between libraries.

Basically, there are two possibilities for running Flowman in AWS EMR: Either use shell access with a traditional
EMR cluster, or create a so-called fat jar from your Flowman project and run it as a Spark execution step in your
EMR cluster. We will discuss both options in detail below.


## Running Flowman via shell access

The first (and a very natural) approach will simply install Flowman on the master node within your EMR cluster.

### 1. Log in to EMR via ssh

As the first step, you need to log in to the master node of the EMR cluster using ssh. Getting ssh access involves 
opening firewall ports, providing your public ssh key and more. You can read detailed instructions at the 
[official AWS documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html).


### 2. Install Flowman on EMR

Once you successfully logged in to the master node, you need to download an EMR optimized version of Flowman using
`wget` or `curl`:
```shell
wget https://github.com/dimajix/flowman/releases/download/1.0.0/flowman-dist-1.0.0-emr6.10-spark3.3-hadoop3.3-bin.tar.gz
```
Next you unpack Flowman as follows:
```shell
tar xvzf flowman-dist-1.0.0-emr6.10-spark3.3-hadoop3.3-bin.tar.gz
```
This will create a directory `flowman-1.0.0-emr6.10-spark3.3-hadoop3.3` which contains all executables and libraries
of Flowman.

#### Directory Layout
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


### 3. Configuration (optional)

You probably need to perform some basic global configuration of Flowman. The relevant files are stored in the `conf`
directory.

#### Configuration of `flowman-env.sh`

The `flowman-env.sh` script sets up an execution environment on a system level. Here some very fundamental Spark
and Hadoop properties can be configured, like for example
* `KRB_PRINCIPAL` and `KRB_KEYTAB` for using Kerberos
* Generic Java options like HTTP proxy and more

```shell
#!/usr/bin/env bash

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
```


#### Configuration of  `default-namespace.yml`
On top of the very global settings, Flowman also supports so-called *namespaces*. Each project is executed within the
context of one namespace, which would be the *default namespace* if nothing else is specified. Each namespace contains
some configuration, such that different namespaces might represent different tenants or different staging environments.

```yaml
name: "default"

config:
  - spark.executor.cores=$System.getenv('SPARK_EXECUTOR_CORES', '8')
  - spark.executor.memory=$System.getenv('SPARK_EXECUTOR_MEMORY', '16g')

plugins:
  - flowman-aws
  - flowman-delta
  - flowman-kafka
  - flowman-mariadb
```

### 4. Copy your Flowman project

Flowman alone will not be too useful, you will also want to bring your Flowman project to AWS. You can either copy your
project files to the master node, or you can also copy them to S3 and have Flowman load them from there.

### 5. Run Flowman

Finally, you can now start Flowman. For example, if you wanted to run the weather example, you can run the Flowman shell
as follows
```shell
# Enter installation directory of Flowman
cd flowman-1.0.0-emr6.10-spark3.3-hadoop3.3

# Start Flowman shell with the weather example
bin/flowshell -f examples/weather
```



## Executing Flowman as a Spark execution step 

The previous approach of logging in to the master node, downloading Flowman and installing Flowman, and then
executing a project is difficult to automate. But EMR also offers so-called step functions, which will be executed
directly after the creation of an EMR cluster. After all step functions (you can specify multiple) have completed, AWS 
can optionally shut down the cluster. Therefore, this approach fits well to periodic batch processes where some Flowman
projects should be executed. 

But running a step function does not work well with the traditional shell based approach of Flowman. Instead, the easiest
way is to create a single jar (Java Archive) file containing all Flowman code, additional libraries and your project. 
Such jar files are called "fat jar", since they tend to be very big.

### 1. Build a fat jar

Normally, one needs to be a Maven expert to build such a fat jar. But not so with Flowman, since it provides a
plugin for Maven, the so-called `flowman-maven-plugin`, which greatly simplifies building such fat jars. One can
find a detailed description for using the plugin at [Using Flowman Maven Plugin](../workflow/maven-plugin.md).

In this case, we need a small `pom.xml` file (this is the build descriptor for Maven), which looks as follows:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>my.company</groupId>
    <artifactId>my-project</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>quickstart</name>

    <properties>
        <encoding>UTF-8</encoding>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <build>
        <plugins>
            <plugin>
                <groupId>com.dimajix.flowman.maven</groupId>
                <artifactId>flowman-maven-plugin</artifactId>
                <version>0.1.0</version>
                <extensions>true</extensions>
                <configuration>
                    <deploymentDescriptor>deployment.yml</deploymentDescriptor>
                </configuration>

                <!-- Additional plugin dependencies for specific deployment targets -->
                <dependencies>
                    <!-- Support for deploying to S3 storage -->
                    <dependency>
                        <groupId>com.dimajix.flowman.maven</groupId>
                        <artifactId>flowman-provider-aws</artifactId>
                        <version>0.1.0</version>
                    </dependency>
                </dependencies>
            </plugin>
        </plugins>
    </build>
</project>
```
You also need an additional Flowman specific file `deployment.yml`, which is referenced by the `pom.xml` file above.
This deployment descriptor contains all required information
```yaml
flowman:
  # Specify the Flowman version to use
  version: 1.0.0-emr6.10-spark3.3-hadoop3.3
  plugins:
    # Specify the list of plugins to use
    - flowman-avro
    - flowman-aws
    - flowman-delta
    - flowman-mariadb
    - flowman-mysql


# List of subdirectories containing Flowman projects
projects:
  - flow


# Specify possibly multiple redistributable packages to be built
packages:
  # This package is called "emr"
  emr:
    # The package is a "fatjar" package, i.e. a single jar file containing both Flowman and your project
    kind: fatjar


# Optional: List of deployments, which will copy packages to their destination
deployments:
  # This deployment is called "aws"
  aws:
    kind: copy
    package: emr
    # This specifies the location where the fatjar will be uploaded to in the "flowman:deploy" step
    location: s3://flowman-test/integration-tests/emr
```
You can then build the package via
```shell
mvn clean install
```
This will create a fat jar `my-project-1.0-SNAPSHOT-emr` in the subdirectory `target/emr`. The general pattern for
the artifact names is `<artifactId>-<version>-<package>`.


### 2. Deploy jar to AWS S3

Once you have built the fat jar, you need to upload it to S3, so AWS EMR can access it. You can either do this
step manually, or you can also use the Flowman Maven plugin to help you:
```shell
mvn flowman:deploy
```

### 3. Execute as Spark step

When defining a cluster, you can specify steps to execute. Here we can add Flowman as a Spark application:

| Setting                 | Description                   | Value                                                                     |
|-------------------------|-------------------------------|---------------------------------------------------------------------------|
| Type                    | The type of the step function | "Spark application"                                                       |
| Deployment mode         | Spark deployment mode         | "client"                                                                  |
| Application location    | Location of the fat jar in S3 | `s3://flowman-test/integration-tests/emr/my-project-1.0-SNAPSHOT-emr.jar` |
| spark-submit parameters | Parameters passed to Spark    | `--class com.dimajix.flowman.tools.exec.Driver`                           |
| Application arguments   | Parameters passed to Flowman  | `-f flow job build main`                                                  |



## Executing Flowman in EMR serverless

Finally, the third option is very similar to the last one, except that we use the *EMR serverless* service. This AWS
service will spin up a Spark cluster just for one specific job (a Flowman job in this case), run the job and then
destroy the cluster again. Spinning up and shutting down is much faster than with traditional EMR clusters, which
makes this service a perfect fit for batch workloads.

### 1. Build a fat jar

First, you have to build a fat jar, like in the previous approach. There is no difference, we skip the details and
refer you to the [previous section](#1-build-a-fat-jar).

### 2. Deploy jar to AWS S3

Again, we need to deploy the fat jar to S3, such that AWS EMR serverless can access it. You can again either manually
copy the jar to S3 or use the Flowman Maven plugin via
```shell
mvn flowman:deploy
```

### 3. Execute in EMR serverless

First you need to create an "application" in EMR Studio, then you can create a "job run", which refers to the Flowman
executable. The following settings are required:

| Setting           | Description                   | Value                                                                     |
|-------------------|-------------------------------|---------------------------------------------------------------------------|
| Type              | The type of the step function | "Spark application"                                                       |
| Deployment mode   | Spark deployment mode         | "client"                                                                  |
| Script location   | Location of the fat jar in S3 | `s3://flowman-test/integration-tests/emr/my-project-1.0-SNAPSHOT-emr.jar` |
| Main class        | The Java class to execute     | `com.dimajix.flowman.tools.exec.Driver`                                   |
| Script arguments  | Parameters passed to Flowman  | `["-B", "-f", "flow", "job", "build", "main"]`                            |

Once you submit the job, and all settings are correct, AWS should spin up a temporary Spark cluster and start Flowman.
