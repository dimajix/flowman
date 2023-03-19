# Flowman Executor (`flowexec`)

`flowexec` is the primary tool for running a whole project, for building individual targets
or for inspecting individual entities.

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
* `-X` or `--verbose` Enables logging at more verbose level
* `-XX` or `--debug` Enables logging at debug level


### Exit Codes

`flowexec` provides different exit codes depending on the result of the execution

| exit code | description                                                                    |
|-----------|--------------------------------------------------------------------------------|
| 0         | Everything worked out nicely, no error. This includes skipped                  |
| 2         | There were individual errors, but the run was successful (Success with Errors) |
| 3         | There were execution errors                                                    |
| 4         | The command line was not correct                                               |
| 5         | An uncaught exception occurred                                                 |


## Commands

All commands for `flowexec` are organized in *command groups*, for example project commands, job commands, target
commands and so on. Please find an overview with links to the detailed documentation below:


```eval_rst
.. toctree::
   :maxdepth: 1

   project
   job
   target
   misc
```
