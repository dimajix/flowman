# Flowman Executor (`flowexec`)

`flowexec` is the primary tool for running a whole project, for building individual targets
or for inspecting individual entities.

### General Parameters
* `-h` displays help
* `-f <project_directory>` specifies a different directory than the current for locating a Flowman project
* `-P <profile_name>` activates a profile as being defined in the Flowman project
* `-D <key>=<value>` Sets an environment variable
* `--conf <key>=<value>` Sets a Flowman or Spark configuration variable
* `--info` Dumps the active configuration to the console
* `--spark-logging <level>` Sets the log level for Spark
* `--spark-master <master>` Explicitly sets the address of the Spark master
* `--spark-name <application_name>` Sets the Spark application name
* `-X` or `--verbose` Enables logging at more verbose level
* `-XX` or `--debug` Enables logging at debug level

### User Settings File
By using the option `-D` and `--conf` you can manually override both Flowman environment and config variables. In
addition to these command line options, Flowman will also look for a file `.flowman-env.yml` in the current working
directory (which may not be the project directory!) for additional overrides. The file `.flowman-env.yml` looks as
follows:
```yaml
environment:
  # Override environment, for example set different directories
  - basedir=file:///tmp/weather
  - srcdir=$System.getenv('WEATHER_SRCDIR', 's3a://dimajix-training/data/weather')

config:
  # Override config, for example inject AWS Credentials
  - spark.hadoop.fs.s3a.access.key=$System.getenv('AWS_ACCESS_KEY_ID')
  - spark.hadoop.fs.s3a.secret.key=$System.getenv('AWS_SECRET_ACCESS_KEY')
```
The recommendation is not to add this file to source control (like git). Instead, developers should maintain their
private copy of this file containing their specific configuration settings (like credentials, local directories etc.).


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
