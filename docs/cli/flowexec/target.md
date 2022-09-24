# `flowexec` Target Commands
It is also possible to perform actions on individual targets using the `target` command group. In most cases this is
inferior to using the `job` interface above, since typical jobs will also define appropriate environment variables
which might be required by targets.

### General Parameters
* `-h` displays help
* `-f <project_directory>` specifies a different directory than the current for locating a Flowman project
* `-P <profile_name>` activates a profile as being defined in the Flowman project
* `-D <key>=<value>` Sets a environment variable
* `--conf <key>=<value>` Sets a Flowman or Spark configuration variable
* `--info` Dumps the active configuration to the console
* `--spark-logging <level>` Sets the log level for Spark
* `--spark-master <master>` Explicitly sets the address of the Spark master
* `--spark-name <application_name>` Sets the Spark application name


## `target list` - List all Targets
In order to retrieve all defined execution targets, run the following command
```shell
flowexec target list
```

## `target <validate|create|build|verify|truncate|destroy>` - Execute Target phase
This set of commands is used for *executing a target phase*, or a complete lifecycle containing multiple individual
phases.
```shell
flowexec target <validate|create|build|verify|truncate|destroy> <target_name>
```
This will execute an individual target by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of the project, even if the output targets already exist.
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
  the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
  `verify`. If this is not what you want, then use the option `-nl`

