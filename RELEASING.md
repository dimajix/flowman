# Releasing

## Preparation

Before performing a release, please make sure
* `CHANGELOG.md` is up to date
* The version is correct in `docs/conf.py`
* You have built the latest development version

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
* `deployment.repository.id` - contains the ID of the repository. This should match any entry in your settings.xml for authentication
* `deployment.repository.snapshot-id` - contains the ID of the repository. This should match any entry in your settings.xml for authentication
* `deployment.repository.server` - the url of the server as used by the nexus-staging-maven-plugin
* `deployment.repository.url` - the url of the default release repository
* `deployment.repository.snapshot-url` - the url of the snapshot repository

Per default, Flowman uses the staging mechanism provided by the nexus-staging-maven-plugin. This this is not what you
want, you can simply disable the Plugin via `skipTests` 

With these settings you can deploy to a different (local) repository, for example

    mvn deploy \
        -PCDP-7.1 \
        -Ddeployment.repository.snapshot-url=https://nexus-snapshots.my-company.net/repository/snapshots \
        -Ddeployment.repository.snapshot-id=nexus-snapshots \
        -DskipStaging \
        -DskipTests
