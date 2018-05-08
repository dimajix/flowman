# Building Flowman

The whole project is built using Maven. The build also includes a Docker image, which requires that Docker
is installed on the build machine.

# Main Artifacts

The main artifacts will be a Docker image 'dimajix/flowman' and additionally a tar.gz file containing a
runnable version of Flowman for direct installation in cases where Docker is not available or when you
want to run Flowman in a complex environment with Kerberos.


# Custom Builds

## Build for Custom Spark Version

Per default, dataflow will be built for fairly recent versions of Spark (2.2.1 as of this writing) and
Hadoop (2.8.3). But of course you can also build for a different version

    mvn install -Dspark.version=2.2.1 -Dhadoop.version=2.7.3
        
## Building for Cloudera

The Maven project also contains preconfigured profiles for Cloudera.

    mvn install -PCDH-5.13        
