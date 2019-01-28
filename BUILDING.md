# Building Flowman

The whole project is built using Maven. The build also includes a Docker image, which requires that Docker
is installed on the build machine.

# Main Artifacts

The main artifacts will be a Docker image 'dimajix/flowman' and additionally a tar.gz file containing a
runnable version of Flowman for direct installation in cases where Docker is not available or when you
want to run Flowman in a complex environment with Kerberos.


# Custom Builds

## Build for Custom Spark Version

Per default, dataflow will be built for fairly recent versions of Spark (2.3.2 as of this writing) and
Hadoop (2.8.3). But of course you can also build for a different version by either using a profile
    
    mvn install -Pspark2.2 -Phadoop2.7
    
This will always select the latest bugfix version within the minor version. You can also specify
versions explicitly as follows:    

    mvn install -Dspark.version=2.2.1 -Dhadoop.version=2.7.3
        
Note that using profiles is the preferred way, as this gurantees that also dependencies are selected
using the correct version.

        
## Building for Cloudera

The Maven project also contains preconfigured profiles for Cloudera.

    mvn install -PCDH-5.13        


# Releasing

## Releasing

## Deploying to Central Repository

Both snapshot and release versions can be deployed to Sonatype, which in turn is mirrored by the Maven Central
Repository.

    mvn deploy
