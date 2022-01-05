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
* `-f` or `--force` force execution of all targets in the job, even if Flowman considers the targets to be clean.
* `-t` or `--targets` explicitly specify targets to be executed. The targets can be specified as regular expressions
* `-d` or `--dirty` explicitly mark individual targets as being dirty, i.e. they need a rebuild. The targets can be
  specified as regular expressions. The difference between `-d` and `-t` is that while `-t` tells Flowman to only rebuild
  the specified targets if they are dirty, `-d` actually taints specific targets as being dirty, i.e. they need a rebuild.
  The difference between `-f` and `-d` is that `-f` marks *all* targets as being dirty, while you can explicitly select
  individual targets with `-d`.
* `-k` or `--keep-going` proceed with execution, in case of errors.
* `-j <n>` runs multiple job instances in parallel. This is very useful for running a job for a whole range of dates.
* `--dry-run` only simulate execution
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
`verify`. If this is not what you want, then use the option `-nl`

The following example will only execute the `BUILD` phase of the job `daily`, which defines a parameter
`processing_datetime` with type datetiem. The job will be executed for the whole date range from 2021-06-01 until 
2021-08-10 with a step size of one day. Flowman will execute up to four jobs in parallel (`-j 4`).

```shell
flowexec job build daily processing_datetime:start=2021-06-01T00:00 processing_datetime:end=2021-08-10T00:00 processing_datetime:step=P1D --target parquet_lineitem --no-lifecycle -j 4
```


## Target Commands
It is also possible to perform actions on individual targets using the `target` command group. In most cases this is
inferior to using the `job` interface above, since typical jobs will also define appropriate environment variables
which might be required by targets.

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
