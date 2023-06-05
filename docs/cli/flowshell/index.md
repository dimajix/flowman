# Flowman Interactive Shell (`flowshell`)

`flowshell` is an interactive shell for inspecting and executing Flowman projects. 

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
*Available since Flowman version 1.0.0*

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


## Commands

All commands within the Flowman Shell mimic the commands of [`flowexec`](../flowexec/index.md). The main difference to multiple
invocations of `flowexec` is the fact that the project is loaded only once and some additional commands are provided.

The commands are organized in command groups
* `info`
* `job`
* `mapping`
* `model` or `relation`
* `namespace`
* `project`
* `target`
* `test`

Some additional commands in `flowshell` which are not available via `flowexec` are
* `exit` or `quit`

## Tutorial

Start the Flowman shell for your project via

```shell
flowshell -f /path/to/your/project
```
    
Now you can list all jobs via

    flowshell> job list

    flowshell> job enter arg1=123
    flowshell> job leave

    flowshell> exit
