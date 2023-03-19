# Flowman Interactive Remote Shell (`flowrshell`)

The Flowman Remote Shell is the client/server pendant of the [Flowman Shell](../flowshell/index.md) to be used
with a [Flowman Kernel Server](../flowman-kernel/index.md). It connects to a running Flowman Kernel Server,
uploads the current local working directory as a (temporary) workspace to the server and then will forward and
command to the server for execution. This means that the Flowman Remote Shell does not perform any local data
transformation, but delegates everything to the kernel. This in turn also means that the Flowman Remote Shell does
only need network access to the kernel and no other services like data sources, a YARN cluster etc.


## General Parameters
* `-h` displays help
* `-k <kernel_url>` specifies the URL of the Flowman Kernel server. Default is `grpc://localhost:8088`.
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


## Commands

Most commands within the Flowman Remote Shell mimic the commands of [flowexec](../flowexec/index.md). The main difference 
to multiple invocations of `flowexec` is the fact that the project is loaded only once and some additional commands are 
provided.

The commands are organized in command groups
* `job`
* `kernel`
* `mapping`
* `model` or `relation`
* `namespace`
* `project`
* `session`
* `target`
* `test`

Some additional commands in `flowshell` which are not available via `flowexec` are
* `exit` or `quit`
