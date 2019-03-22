# Building Flowman

The whole project is built using Maven. The build also includes a Docker image, which requires that Docker
is installed on the build machine.

# Main Artifacts

The main artifacts will be a Docker image 'dimajix/flowman' and additionally a tar.gz file containing a
runnable version of Flowman for direct installation in cases where Docker is not available or when you
want to run Flowman in a complex environment with Kerberos.


# Custom Builds

## Build on Windows

Although you can normally build Flowman on Windows, you will need the Hadoop WinUtils installed. You can download
the binaries from https://github.com/steveloughran/winutils and install an appropriate version somewhere onto your 
machine. Do not forget to set the HADOOP_HOME environment variable to the installation directory of these utils!

You should also configure git such that all files are checked out using "LF" endings instead of "CRLF", otherwise
some unittests may fail and Docker images might not be useable. This can be done by setting the git configuration
value "core.autocrlf" to "input"

    git config --global core.autocrlf input


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

### 1. Make a release to the master branch 
    mvn gitflow:release
    
### 2. Deploy artefacts to Sonatype repository    
    mvn deploy
    
The deployment has to be committed via     
    
    mvn nexus-staging:close -DstagingRepositoryId=comdimajixflowman-1001
    
Or the staging data can be removed via

    mvn nexus-staging:drop    
