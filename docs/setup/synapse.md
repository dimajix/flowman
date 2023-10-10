# Deploying Flowman to Azure Synapse

Since version 1.0.0, Flowman officially supports Azure Synapse as an execution environment. In order
to provide a high degree of compatibility, Flowman provides a special build variant for Azure Synapse, which can be
identified via the term "synapse" in their version.

For example, the Flowman version `1.0.0-synapse3.3-spark3.3-hadoop3.3` contains a build specifically crafted for Azure
Synapse version 3.3, which contains Spark 3.3 and Hadoop 3.3. You should always use a Flowman version which matches your
Synapse version (or the other way round) in order to avoid incompatibilities between libraries.


## Executing Flowman in Azure Synapse Spark

Running Flowman in Azure Synapse does not support the traditional shell based approach of Flowman. Instead, the easiest
way is to create a single jar (Java Archive) file containing all Flowman code, additional libraries and your project.
Such jar files are called "fat jar", since they tend to be very big.

### 1. Build a fat jar

Normally, you need to be a Maven expert to build such a fat jar. But not so with Flowman, since it provides a
plugin for Maven, the so-called `flowman-maven-plugin`, which greatly simplifies building such fat jars. You can
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
                <version>0.4.0</version>
                <extensions>true</extensions>
                <configuration>
                    <deploymentDescriptor>deployment.yml</deploymentDescriptor>
                </configuration>

                <!-- Additional plugin dependencies for specific deployment targets -->
                <dependencies>
                    <!-- Support for deploying to S3 storage -->
                    <dependency>
                        <groupId>com.dimajix.flowman.maven</groupId>
                        <artifactId>flowman-provider-azure</artifactId>
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
  version: 1.1.0-synapse3.3-spark3.3-hadoop3.3
  plugins:
    # Specify the list of plugins to use
    - flowman-avro
    - flowman-aws
    - flowman-azure
    - flowman-delta
    - flowman-mssqlserver


# List of subdirectories containing Flowman projects
projects:
  - flow


# Specify possibly multiple redistributable packages to be built
packages:
  # This package is called "synapse"
  synapse:
    # The package is a "fatjar" package, i.e. a single jar file containing both Flowman and your project
    kind: fatjar


# Optional: List of deployments, which will copy packages to their destination
deployments:
  # This deployment is called "synapse"
  synapse:
    kind: copy
    package: synapse
    # This specifies the location where the fatjar will be uploaded to in the "flowman:deploy" step
    location: abfs://flowman@dimajixspark.dfs.core.windows.net/integration-tests
```
You can then build the package via
```shell
mvn clean install
```
This will create a fat jar `my-project-1.0-SNAPSHOT-synapse` in the subdirectory `target/synapse`. The general pattern for
the artifact names is `<artifactId>-<version>-<package>`.

### 2. Deploy jar to Azure Blob Storage

Once you have built the fat jar, you need to upload it to Azure Blob Storage, so Azure can access it. You can either do this
step manually, or you can also use the Flowman Maven plugin to help you:
```shell
mvn flowman:deploy
```

### 3. Execute in Azure Synapse

First you need to create an "Apache Spark Pool" in Azure Synapse with the correct Spark version, which would be 3.3
as the time of writing. Then you need to create an "Apache Spark job definition" in Azure Synapse Workspace with the
following properties:

| Setting                | Description                                                          | Value                                                                                                     |
|------------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| Language               | The type of the Spark application, either Python or Scala            | "Scala"                                                                                                   |
| Main job file          | Location of the fat jar in Azure Blob Storage                        | `abfss://flowman@dimajixspark.dfs.core.windows.net/integration-tests/my-project-1.0-SNAPSHOT-synapse.jar` |
| Main class             | The Java/Scala class to execute                                      | `com.dimajix.flowman.tools.exec.Driver`                                                                   |
| Command line arguments | Command line parameters passed to Flowman                            | `-B -f flow job build main --force`                                                                       |
| Apache Spark pool      | Spark pool to use for this job. Needs to provide the correct version | *name of your Spark pool*                                                                                 |

Once you submit the job, and all settings are correct, AWS should spin up a temporary Spark cluster and start Flowman.
