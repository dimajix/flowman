# Building Flowman

The whole project is built using Maven. The build also includes a Docker image, which requires that Docker
is installed on the build machine.

### Prerequisites

You need the following tools installed on your machine:
* JDK 11 or later - but not too new (Java 16 is currently not supported)
* Apache Maven (install via package manager download from https://maven.apache.org/download.cgi)
* npm (install via package manager or download from https://www.npmjs.com/get-npm)
* Docker (the creation of the Docker image can be skipped via `-Ddockerfile.skip`)
* Windows users also need Hadoop winutils installed. Those can be retrieved from https://github.com/cdarlint/winutils
and later. See some additional details for building on Windows below.
  

## Build with Maven

Building Flowman with the default settings (i.e. newest supported Spark and Hadoop versions will be used) is as easy as

```shell
mvn clean install
```

### Main Artifacts

The main artifacts will be a Docker image 'dimajix/flowman' and additionally a tar.gz file containing a runnable 
version of Flowman for direct installation in cases where Docker is not available or when you want to run Flowman 
in a complex environment with Kerberos. You can find the `tar.gz` file in the directory `flowman-dist/target`

### Skip Tests

In case you don't want to run tests, you can simply append `-DskipTests`

```shell
mvn clean install -DskipTests
```

### Skip Docker Image

In case you don't want to build the Docker image (for example when the build itself is done within a Docker container), 
you can simply append `-Ddockerfile.skip`

```shell
mvn clean install -Ddockerfile.skip
```


## Custom Builds

Flowman supports various versions of Spark and Hadoop to match your requirements and your environment. By providing
appropriate build profiles, you can easily create a custom build.

### Build on Windows

Although you can normally build Flowman on Windows, it is recommended to use Linux instead. But nevertheless Windows
is still supported to some extend, but requires some extra care. You will need the Hadoop WinUtils installed. You can 
download the binaries from https://github.com/cdarlint/winutils and install an appropriate version somewhere onto 
your machine. Do not forget to set the `HADOOP_HOME` or `PATH` environment variable to the installation directory of these 
utils!

You should also configure git such that all files are checked out using "LF" endings instead of "CRLF", otherwise
some unittests may fail and Docker images might not be useable. This can be done by setting the git configuration
value "core.autocrlf" to "input"

```shell
git config --global core.autocrlf input
```

   
It may well be the case that some unittests fail on Windows - don't panic, we focus on Linux systems and ensure that
the `master` branch really builds clean with all unittests passing on Linux.


### Build for Custom Spark / Hadoop Version

Per default, Flowman will be built for fairly recent versions of Spark (3.0.2 as of this writing) and Hadoop (3.2.0). 
But of course you can also build for a different version by either using a profile

```shell
mvn install -Pspark2.4 -Phadoop2.7 -DskipTests
```
 
This will always select the latest bugfix version within the minor version. You can also specify versions explicitly 
as follows:    

```shell
mvn install -Dspark.version=2.4.3 -Dhadoop.version=2.7.3
```
        
Note that using profiles is the preferred way, as this guarantees that also dependencies are selected
using the correct version. The following profiles are available:

* spark-2.4
* spark-3.0
* spark-3.1 
* spark-3.2
* spark-3.3
* hadoop-2.6
* hadoop-2.7
* hadoop-2.8
* hadoop-2.9
* hadoop-3.1
* hadoop-3.2
* hadoop-3.3
* EMR-6.10
* synapse-3.3
* CDH-6.3
* CDP-7.1
* CDP-7.1-spark-3.2
* CDP-7.1-spark-3.3

With these profiles it is easy to build Flowman to match your environment. 


### Building for specific Java Version

If nothing else is set on the command line, Flowman will now build for Java 11 (except when building the profile
CDH-6.3, where Java 1.8 is used). If you are still stuck on Java 1.8, you can simply override the Java version by 
specifying the property `java.version`

```shell
mvn install -Djava.version=1.8
```


### Building for Open Source Hadoop and Spark

#### Spark 2.4 and Hadoop 2.6:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.6 -DskipTests
```

#### Spark 2.4 and Hadoop 2.7:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.7 -DskipTests
```

#### Spark 2.4 and Hadoop 2.8:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.8 -DskipTests
```

#### Spark 2.4 and Hadoop 2.9:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.9 -DskipTests
```

#### Spark 3.0 and Hadoop 3.1

```shell
mvn clean install -Pspark-3.0 -Phadoop-3.1 -DskipTests
```

#### Spark 3.0 and Hadoop 3.2

```shell
mvn clean install -Pspark-3.0 -Phadoop-3.2 -DskipTests
```

#### Spark 3.1 and Hadoop 2.7

```shell
mvn clean install -Pspark-3.1 -Phadoop-2.7 -DskipTests
```

#### Spark 3.1 and Hadoop 3.2

```shell
mvn clean install -Pspark-3.1 -Phadoop-3.2 -DskipTests
```

#### Spark 3.2 and Hadoop 2.7

```shell
mvn clean install -Pspark-3.2 -Phadoop-2.7 -DskipTests
```

#### Spark 3.2 and Hadoop 3.3

```shell
mvn clean install -Pspark-3.2 -Phadoop-3.3 -Dhadoop.version=3.3.1 -DskipTests
```

#### Spark 3.3 and Hadoop 2.7

```shell
mvn clean install -Pspark-3.3 -Phadoop-2.7 -DskipTests
```

#### Spark 3.3 and Hadoop 3.3

```shell
mvn clean install -Pspark-3.3 -Phadoop-3.3 -Dhadoop.version=3.3.2 -DskipTests
```


### Building for Cloudera

The Maven project also contains preconfigured profiles for Cloudera CDH 6.3 and for Cloudera CDP 7.1 and also supports
optional Spark 3.2 and Spark 3.3 parcels for CDP 7.1:
```shell
mvn clean install -PCDH-6.3 -DskipTests
```
```shell
mvn clean install -PCDP-7.1 -DskipTests
```
```shell
mvn clean install -PCDP-7.1-spark-3.2 -DskipTests
```
```shell
mvn clean install -PCDP-7.1-spark-3.3 -DskipTests
```

### Building for AWS EMR

The Maven project also contains preconfigured profiles for AWS EMR 6.10.
```shell
mvn clean install -PEMR-6.10 -DskipTests
```

### Building for Azure Synapse
Flowman also provides a build profile for Azure Synapse 3.3:
```shell
mvn clean install -Psynapase-3.3 -DskipTests
```


## Coverage Analysis

Flowman also now supports creating a coverage analysis via the scoverage Maven plugin. It is not part of the default
build and has to be triggered explicitly:

```shell
mvn scoverage:report
```


## Building Documentation

Flowman also contains Markdown documentation which is processed by Sphinx to generate the online HTML documentation.

    cd docs
    make html
