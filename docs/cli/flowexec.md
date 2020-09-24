# Flowman Executor (flowexec)

`flowexec` is the primary tool for running a whole project, for building individual targets
or for inspecting individual entities.

## General Parameters
* `-h` displays help
* `-f <project_directory>` specifies a different directory than the current for locating a Flowman project
* `-P <profile_name>` activates a profile as being defined in the Flowman project
* `-D <key>=<value>` Sets a environment variable
* `--conf <key>=<value>` Sets a Flowman or Spark configuration variable
* `--info` Dumps the active configuration to the console
* `--spark-logging <level>` Sets the log level for Spark 
* `--spark-master <master>` Explicitly sets the address of the Spark master
* `--spark-name <application_name>` Sets the Spark application name


## Project Commands
The most important command group is for executing a specific lifecycle or a individual phase for the whole project.
```shell script
flowexec project <create|build|verify|truncate|destroy> <args>
```
This will execute the whole job by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of the project, even if the output targets already exist.
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
`verify`. If this is not what you want, then use the option `-nl`


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
This will execute the whole job by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of the project, even if the output targets already exist.
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
`verify`. If this is not what you want, then use the option `-nl`


## Target Commands
It is also possible to perform actions on individual targets using the `target` command group.

### List Targets
```shell script
flowexec target list
```

### Execute Target phase
```shell script
flowexec target <create|build|verify|truncate|destroy> <target_name>
```
This will execute an individual target by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of the project, even if the output targets already exist.
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
`verify`. If this is not what you want, then use the option `-nl`


## Info Command
As a small debugging utility, Flowman also provides an `info` command, which simply shows all environment variables
and configuration settings.
```shell script
flowexec info
```
