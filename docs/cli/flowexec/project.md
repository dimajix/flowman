# `flowexec` Project Commands

By using project commands with `flowexec`, you can perform operations on the project level. Most of these operations
implicitly reference a `main` job, which must be defined within the project

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


## `project <verify|create|build|verify|truncate|destroy>` - Lifecycle Commands
The most important command group is for executing a specific lifecycle or an individual phase for the whole project.
```shell
flowexec project <verify|create|build|verify|truncate|destroy> <args>
```
This will execute the whole job by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of all targets in the project, even if Flowman considers the targets to be clean.
* `-t` or `--targets` explicitly specify targets to be executed. The targets can be specified as regular expressions.
* `-d` or `--dirty` explicitly mark individual targets as being dirty, i.e. they need a rebuild. The targets can be
  specified as regular expressions. The difference between `-d` and `-t` is that while `-t` tells Flowman to only rebuild
  the specified targets if they are dirty, `-d` actually taints specific targets as being dirty, i.e. they need a rebuild.
  The difference between `-f` and `-d` is that `-f` marks *all* targets as being dirty, while you can explicitly select
  individual targets with `-d`.
* `-k` or `--keep-going` proceed with execution, in case of errors.
* `-j <n>` or `--jobs <n>` execute multiple jobs in parallel
* `--dry-run` only simulate execution
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceding phases. For example
  the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
  `verify`. If this is not what you want, then use the option `-nl`

### Examples
In order to build a project (i.e. run `VALIDATE`, `CREATE` and `BUILD` execution phases) stored in the subdirectory
`examples/weather` which defines an (optional) parameter `year`, simply run

```shell
flowexec -f examples/weather project build year=2018
```

If you only want to execute the `BUILD` phase and skip the first two other phases, then you need to add the
command line option `-nl` to skip the lifecycle:

```shell
flowexec -f examples/weather project build year=2018 -nl
```

