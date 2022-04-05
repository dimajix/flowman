# Running Flowman on Windows

Flowman is best run on Linux, especially for production usage. Windows support is at best experimental and will
probably be never within the focus of the project. Nevertheless, there are also some options for running Flowman on
Windows, with the main purpose to provide developers some way to create and test projects on their local machines.

The main difficulty in supporting Windows comes from two aspects
* Windows doesn't support `bash`, therefore all scripts have been rewritten to run on Windows
* Hadoop and Spark require some special *Hadoop WinUtils* libraries to be installed


## Installing using WinUtils
The first naturla option is to install Flowman directly on your Windows machine. Of course this also requires a
working Apache Spark installation. You can download an appropriate version from the
[Apache Spark homepage](https://spark.apache.org).

Next, you are required to install the *Hadoop Winutils*, which is a set of DLLs required for the Hadoop libraries to
be working. You can get a copy at https://github.com/kontext-tech/winutils .
Once you downloaded the appropriate version, you need to place the DLLs into a directory `$HADOOP_HOME/bin`, where
`HADOOP_HOME` refers to some arbitrary location of your choice on your Windows PC. You also need to set the following
environment variables:
* `HADOOP_HOME` should point to the parent directory of the `bin` directory
* `PATH` should also contain `$HADOOP_HOME/bin`


## Using Docker
A simpler way to run Flowman on Windows is to use a Docker image available on
[Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman)


## Using WSL
And of course you can also simply install a Linux distro of your choice via WSL and then normally 
[install Flowman](../setup/installation.md) within WSL.
