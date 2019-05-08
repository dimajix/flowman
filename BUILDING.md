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
    
You might also want to skip unittests (the HBase plugin is currently failing under windows)

    mvn clean install -DskipTests    


## Build for Custom Spark Version

Per default, dataflow will be built for fairly recent versions of Spark (2.3.2 as of this writing) and
Hadoop (2.8.3). But of course you can also build for a different version by either using a profile
    
    mvn install -Pspark2.2 -Phadoop2.7 -DskipTests
    
This will always select the latest bugfix version within the minor version. You can also specify
versions explicitly as follows:    

    mvn install -Dspark.version=2.2.1 -Dhadoop.version=2.7.3
        
Note that using profiles is the preferred way, as this gurantees that also dependencies are selected
using the correct version.

        
## Building for Cloudera

The Maven project also contains preconfigured profiles for Cloudera.

    mvn install -Pspark-2.3 -PCDH-5.15 -DskipTests


# Releasing

## Releasing

When making a release, the gitflow maven plugin should be used for managing versions

    mvn gitflow:release

## Deploying to Central Repository

Both snapshot and release versions can be deployed to Sonatype, which in turn is mirrored by the Maven Central
Repository.

    mvn deploy -Dgpg.skip=false
    
The deployment has to be committed via     
    
    mvn nexus-staging:close -DstagingRepositoryId=comdimajixflowman-1001
    
Or the staging data can be removed via

    mvn nexus-staging:drop    

## Deploying to Custom Repository

You can also deploy to a different repository by setting the following properties
* `deployment.repository.id` - contains the ID of the repository. This should match any entry in your settings.xml
  for authentication
* `deployment.repository.server` - the url of the server as used by the nexus-staging-maven-plugin
* `deployment.repository.url` - the url of the default release repsotiory
* `deployment.repository.snapshot-url` - the url of the snapshot repository

Per default, Flowman uses the staging mechanism provided by the nexus-staging-maven-plugin. This this is not what you
want, you can simply disable the Plugin via `skipTests` 

With these settings you can deploy to a different (local) repository, for example

    mvn deploy \
        -Pspark-2.3 \
        -PCDH-5.15 \
        -Ddeployment.repository.snapshot-url=https://nexus-snapshots.my-company.net/repository/snapshots \
        -Ddeployment.repository.id=nexus-snapshots \
        -DskipStaging \
        -DskipTests
