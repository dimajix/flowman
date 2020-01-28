# Flowman Executor (flowexec)

`flowexec` is the primary tool for running a whole project, for building individual targets
or for inspecting individual entitites.

## General Parameters
* `-h` displays help
* `-f <project_directory>` specifies a different directory than the current for locating a Flowman project
* `-P <profile_name>` activates a profile as being defined in the Flowman project
* `-D <key>=<value>` Sets a environment variable
* `--conf <key>=<value>` Sets a Flowman or Spark configuration variable
* `--info` Dumps the active configuration to the console
* `--spark-logging <level>` Sets the log level for Spark 
* `--spark-name <application_name>` Sets the Spark application name


## Project Commands
The most important command group is for executing a specific lifecycle or a individual phase for the whole project.
```shell script
flowexec project <create|build|verify|truncate|destroy> <args>
```
This will execute the whole job by executing the desired lifecycle for the `main` job


## Job Commands
Similar to the project commands, individual jobs with different names than `main` can be executed.

### List Jobs
The following command will list all jobs defined in a project
```shell script
flowexec job list
```

### Execute Job phase
```shell script
flowexec job <create|build|verify|truncate|destroy> <job_name> <args>
```

## Target Commands

### List Targets
```shell script
flowexec target list
```

### Execute Target phase
```shell script
flowexec job <create|build|verify|truncate|destroy> <job_name> <args>
```
