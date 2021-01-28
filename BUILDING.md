# Building Flowman

The whole project is built using Maven. The build also includes a Docker image, which requires that Docker
is installed on the build machine.

## Build with Maven

Building Flowman with the default settings (i.e. Hadoop and Spark version) is as easy as

```shell
mvn clean install
```

## Main Artifacts

The main artifacts will be a Docker image 'dimajix/flowman' and additionally a tar.gz file containing a runnable 
version of Flowman for direct installation in cases where Docker is not available or when you want to run Flowman 
in a complex environment with Kerberos. You can find the `tar.gz` file in the directory `flowman-dist/target`


# Custom Builds

## Build on Windows

Although you can normally build Flowman on Windows, you will need the Hadoop WinUtils installed. You can download
the binaries from https://github.com/steveloughran/winutils and install an appropriate version somewhere onto your 
machine. Do not forget to set the HADOOP_HOME environment variable to the installation directory of these utils!

You should also configure git such that all files are checked out using "LF" endings instead of "CRLF", otherwise
some unittests may fail and Docker images might not be useable. This can be done by setting the git configuration
value "core.autocrlf" to "input"

```shell
git config --global core.autocrlf input
```

You might also want to skip unittests (the HBase plugin is currently failing under windows)

```shell
mvn clean install -DskipTests
```
    
It may well be the case that some unittests fail on Windows - don't panic, we focus on Linux systems and ensure that
the `master` branch really builds clean with all unittests passing on Linux.


## Build for Custom Spark / Hadoop Version

Per default, Flowman will be built for fairly recent versions of Spark (2.4.5 as of this writing) and Hadoop (2.8.5). 
But of course you can also build for a different version by either using a profile

```shell
mvn install -Pspark2.3 -Phadoop2.7 -DskipTests
```
 
This will always select the latest bugfix version within the minor version. You can also specify versions explicitly 
as follows:    

```shell
mvn install -Dspark.version=2.2.1 -Dhadoop.version=2.7.3
```
        
Note that using profiles is the preferred way, as this guarantees that also dependencies are selected
using the correct version. The following profiles are available:

* spark-2.3
* spark-2.4
* spark-3.0
* hadoop-2.6
* hadoop-2.7
* hadoop-2.8
* hadoop-2.9
* hadoop-3.1
* hadoop-3.2
* CDH-5.15
* CDH-6.3

With these profiles it is easy to build Flowman to match your environment. 

## Building for Open Source Hadoop and Spark

### Spark 2.3 and Hadoop 2.6:

```shell
mvn clean install -Pspark-2.3 -Phadoop-2.6
```

### Spark 2.3 and Hadoop 2.7:

```shell
mvn clean install -Pspark-2.3 -Phadoop-2.7
```

### Spark 2.3 and Hadoop 2.8:

```shell
mvn clean install -Pspark-2.3 -Phadoop-2.8
```

### Spark 2.3 and Hadoop 2.9:

```shell
mvn clean install -Pspark-2.3 -Phadoop-2.9
```

### Spark 2.4 and Hadoop 2.6:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.6
```

### Spark 2.4 and Hadoop 2.7:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.7
```

### Spark 2.4 and Hadoop 2.8:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.8
```

### Spark 2.4 and Hadoop 2.9:

```shell
mvn clean install -Pspark-2.4 -Phadoop-2.9
```

### Spark 3.0 and Hadoop 3.1

```shell
mvn clean install -Pspark-3.0 -Phadoop-3.1
```

### Spark 3.0 and Hadoop 3.2

```shell
mvn clean install -Pspark-3.0 -Phadoop-3.2
```

## Building for Cloudera

The Maven project also contains preconfigured profiles for Cloudera.

```shell
mvn clean install -Pspark-2.3 -PCDH-5.15 -DskipTests
```
    
Or for Cloudera 6.3 

```shell
mvn clean install -Pspark-2.4 -PCDH-6.3 -DskipTests
```

# Coverage Analysis
```shell
mvn scoverage:report
```

# Building Documentation

Flowman also contains Markdown documentation which is processed by Sphinx to generate the online HTML documentation.

    cd docs
    make html
