# Development & Deployment Workflow

This text will give you some guidance on how a typical development workflow would look like. Unsurprisingly, the steps
should look very familiar since they represent the typical development workflow based on local development, CI/CD and
production deployment. The workflow starts from creating
a new Flowman project, describes how to run the project locally, build a self-contained redistributable package
and then deploy it to a remote repository manager like Nexus.

![Flowman Development Workflow](../images/flowman-workflow.png)

The whole workflow is implemented with [Apache Maven](https://maven.apache.org/), but you could of course also chose a 
different build tool. Maven was chosen simply because one can assume that this is present in a Big Data environment, so 
no additional installation on developer machines or CI/CD infrastructure is required.

There are different approaches possible, even within the Maven universe. 

* [Using Flowman Maven plugins](maven-plugin.md). The first approach uses Maven as the build system together with
  the Flowman Maven plugin. This is the preferred archetype as it simplifies creating appropriate artifacts like Flowman
  distributions or fat jars.
* [Using Maven standard plugins](maven-classic.md). The second approach will also use Maven, but only together with 
  standard Maven plugins like the maven-dependency-plugin and the maven-assembly-plugin.
  This approach will only create a Flowman distribution `tar.gz` and cannot be used out of the box for creating a fat
  jar. Of course, explicitly using standard Maven plugins offers more flexibility, but also requires more expertise for
  working with Maven.
