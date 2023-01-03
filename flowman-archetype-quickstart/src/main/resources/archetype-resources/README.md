# Flowman Quickstart Project

This is the quickstart Flowman project, created from a Maven archetype. 

### Contents
The project provides a skeleton structure with the following entities:
* A couple of relations (one source `measurements_raw` and two sinks `measurements` and `measurements_raw`)
* A couple of mapping to extract measurement information from `measurements_raw`
* Two targets for writing the extracted measurements as files and to a JDBC database
* One `main` job containing both targets
* A small test suite in the `flow/test` directory
* Some configuration options in the `flow/config` directory
```
├── conf
│   ├── default-namespace.yml
│   └── flowman-env.sh
├── flow
│   ├── config
│   │   ├── aws.yml
│   │   ├── config.yml
│   │   ├── connections.yml
│   │   └── environment.yml
│   ├── documentation.yml
│   ├── job
│   │   └── main.yml
│   ├── mapping
│   │   └── measurements.yml
│   ├── model
│   │   ├── measurements-raw.yml
│   │   └── measurements.yml
│   ├── project.yml
│   ├── schema
│   │   └── measurements.json
│   ├── target
│   │   ├── documentation.yml
│   │   └── measurements.yml
│   └── test
│       └── test-measurements.yml
├── assembly.xml
├── pom.xml
└── README.md
```

This should give you a starting point to create your own data flow with all required components.


## 1. Developing

For doing local development, it is recommended to either locally install Flowman (a good solution on a Linux PC)
or use a Docker image (the preferred way on a Windows PC).

### Local Installation
For a lcoal installation of Flowman, please follow the steps at https://docs.flowman.io/en/latest/setup/installation.html .

### Using Docker
Flowman provides complete Docker images on Docker Hub, which can be readily used for doing local development. By
mounting the source directory as a volume into the Docker container, you can still use the editor of your choice,
and then use the Flowman Shell for interactive development.
```shell
docker run --rm -ti --mount type=bind,source=./flow,target=/opt/flowman/project dimajix/flowman:0.30.0 bash
```


## 2. Building

You can *build* the project with Apache Maven. This will process all files in the `flow` and `conf` directory and
perform some variable replacements, then it will execute all project tests and finally it will build a deployable
package containing Flowman and the project itself.

In order to run the build, simply execute
```shell
mvn clean install
```


## 3. Installing

Once you have successfully built the project, you will find the redistributable package at
`target/<your-artifactId>-dist-bin.tar.gz`. You can extract this package somewhere onto your local machine,
for example via
```shell
tar xvzf target/<your-artifactId>-<version>-dist-bin.tar.gz
```
This will create a subdirectory `<your-artifactId>-<version>` containg a full Flowman installation including your
project. You then can run the project from inside the folder. 


## 4. Running

For executing the project, simple change into the extracted directory, and execute the desired tasks either via
`flowexec` or inside the Flowman Shell.


### Running all Tests

If you have Flowman installed locally on your machine, you can execute the project via
```shell
bin/flowexec -f flow test run
```
Or you can of course also start the Flowman Shell via
```shell
bin/flowshell -f flow
```

### Building all Targets

If you have Flowman installed locally on your machine, you can execute the project via
```shell
bin/flowexec -f flow job build main year=2013 --force
```


## 5. Deploying

For deploying the Flowman project to your system environment, the generaed package needs to be transferred to your
development/test/production environment and executed. Depending on your environment, some adjustments might be
required, like adding a valid Kerberos principal in the `conf/flowman-env.sh` file.

Once the package is installed, you can then execute the project again by using `flowexec`
```shell
bin/flowexec -f flow job build main year=2013 --force
```
