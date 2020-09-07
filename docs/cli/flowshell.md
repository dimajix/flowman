# Flowman Interactive Shell (flowshell)

`flowshell` is an interactive shell for inspecting and executing Flowman projects. 

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


# Commands

All commands within the Flowman Shell mimic the commands of [flowexec](flowexec.md). The main difference to multiple
invocations of `flowexec` is the fact that the project is loaded only once and some additional commands are provided.

The commands are organized in command groups
* `info`
* `job`
* `mapping`
* `model` or `relation`
* `namespace`
* `project`
* `target`

Some additional commands in `flowshell` which are not available via `flowexec` are
* `exit` or `quit`


## Tutorial

Start the Flowman shell for your project via

    flowshell -f /path/to/your/project
    
Now you can list all jobs via

    flowshell> job list

    flowshell> job enter arg1=123
    flowshell> job leave

    flowshell> exit
