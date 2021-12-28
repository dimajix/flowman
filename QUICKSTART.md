# Flowman Quickstart Guide

# 1. Installation

In order to run the example, you need to have valid access credentials to AWS, since we will be using some data
stored in S3.

## 1.1 Install Spark

Although Flowman directly builds upon the power of Apache Spark, it does not provide a working Hadoop or Spark
environment — and there is a good reason for that: In many environments (specifically in companies using Hadoop
distributions) a Hadoop/Spark environment is already provided by some platform team. And Flowman tries its best not
to mess this up and instead requires a working Spark installation.

Fortunately, Spark is rather simple to install locally on your machine:

### Download & Install Spark

As of this writing, the latest release of Flowman is 0.18.0 and is available prebuilt for Spark 3.1.2 on the Spark
homepage. So we download the appropriate Spark distribution from the Apache archive and unpack it.

```shell
# Create a nice playground which doesn't mess up your system
mkdir playground
cd playground# Download and unpack Spark & Hadoop

curl -L https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz | tar xvzf -# Create a nice link
ln -snf spark-3.1.2-bin-hadoop3.2 spark
```
The Spark package already contains Hadoop, so with this single download you already have both installed and integrated with each other.

### Download & Install Hadoop Utils for Windows

If you are trying to run the application on Windows, you also need the *Hadoop Winutils*, which is a set of
DLLs required for the Hadoop libraries to be working. You can get a copy at https://github.com/kontext-tech/winutils .
Once you downloaded the appropriate version, you need to place the DLLs into a directory `$HADOOP_HOME/bin`, where
`HADOOP_HOME` refers to some location on your Windows PC. You also need to set the following environment variables:
* `HADOOP_HOME` should point to the parent directory of the `bin` directory
* `PATH` should also contain `$HADOOP_HOME/bin`


## 1.2 Install Flowman

You find prebuilt Flowman packages on the corresponding release page on GitHub. For this quickstart, we chose
`flowman-dist-0.14.2-oss-spark3.0-hadoop3.2-bin.tar.gz` which nicely fits to the Spark package we just downloaded before.

```shell
# Download and unpack Flowman
curl -L https://github.com/dimajix/flowman/releases/download/0.18.0/flowman-dist-0.18.0-oss-spark3.1-hadoop3.2-bin.tar.gz | tar xvzf -# Create a nice link
ln -snf flowman-0.18.0 flowman
```

### Flowman Configuration

Now before you can use Flowman, you need to tell it where it can find the Spark home directory which we just created
in the previous step. This can be either done by providing a valid configuration file in
`flowman/conf/flowman-env.sh` (a template can be found at `flowman/conf/flowman-env.sh.template` ), or we can simply
set an environment variable. For the sake of simplicity, we follow the second approach

```shell
# This assumes that we are still in the directory "playground"
export SPARK_HOME=$(pwd)/spark
```

In order to access S3 in the example below, we also need to provide a default namespace which contains some basic
plugin configurations. We simply copy the provided template as follows:

```shell
# Copy default namespace
cp flowman/conf/default-namespace.yml.template flowman/conf/default-namespace.yml
cp flowman/conf/flowman-env.sh.template flowman/conf/flowman-env.sh

export AWS_ACCESS_KEY_ID=<your aws access key>
export AWS_SECRET_ACCESS_KEY=<your aws secret key>
```
That’s all we need to run the Flowman example.


# 2. Flowman Shell

The example data is stored in a S3 bucket provided by myself. In order to access the data, you need to provide valid
AWS credentials in your environment:

```shell
$ export AWS_ACCESS_KEY_ID=<your aws access key>
$ export AWS_SECRET_ACCESS_KEY=<your aws secret key>
```

## 2.1 Start interactive Flowman shell

We start Flowman by running the interactive Flowman shell. While this is not the tool that would be used in automatic
batch processing ( flowexec is the right tool for that scenario), it gives us a good idea how ETL projects in Flowman
are organized.

```shell
cd flowman
bin/flowshell -f examples/weather
```

## 2.2 Inspecting Relations

Now we can inspect some of the relations defined in the project. First we list all relations
```
flowman:weather> relation list
```

Now we can peek inside the relations `stations-raw` and `measurements-raw`. Since the second relation is partitioned
by years, we explicitly specify the year via the option `-p year=2011`
```
flowman:weather> relation show stations-raw
flowman:weather> relation show measurements-raw -p year=2011
```

## 2.3 Running a Job

Now we want to execute the projects main job. Again the job is parametrized by year, so we need to specify the year
that we'd like to process.
```
flowman:weather> job build main year=2011
```

## 2.4 Inspecting Mappings

Now we'd like to inspect some of the mappings which have been used during job execution. Since some mappings depend
on job-specific variables, we need to create a *job context*, which can be done by `job enter <job-name> <job-args>`
as follows:
```
flowman:weather> job enter main year=2011
```
Note how the prompt has changed and will now include the job name. Now we can inspect some mappings:
```
flowman:weather/main> mapping list
flowman:weather/main> mapping show measurements-raw
flowman:weather/main> mapping show measurements-extracted
flowman:weather/main> mapping show stations-raw
```
Finally we'd like to leave the job context again.
```
flowman:weather/main> job leave
```


## 2.5 Inspect Results

The job execution has written its results into some relations again. We can now inspect them again
```
flowman:weather> relation show stations
flowman:weather> relation show measurements
flowman:weather> relation show aggregates -p year=2011
```

## 2.6 History

Flowman also provides an execution history. In the trivial deployment, this information is stored locally in a
Derby database, but other databases like MySQL, MariaDB etc are also supported.
```
flowman:weather> history job search
flowman:weather> history target search -J 1
```

## 2.7 Quitting

Finally we quit the Flowman shell via the `quit` command.
```
flowman:weather> quit
```

# 3. Flowman Execution

So far we have only used the Flowman shell for interactive work with projects. Actually, the shell was developed as a
second step to help analyzing problems and debugging data flows. The primary command for working with Flowman projects
is `flowexec` which is used for non-interactive batch execution, for example within cron-jobs.

It shares a lot of code with the Flowman shell, so the commands are often exactly the same. The main difference is
that with `flowexec` you specify the commands on the command line while `flowshell` provides its own prompt.

For example for running the “build” lifecycle of the weather project for the year 2014, you only need to run:
```shell
bin/flowexec -f examples/weather job build main year=2014
```

# 4. Closing

A very special *Thank You!* goes to all of you who try to follow the example hands-on on your local machine. If you have
problems with following the example, please leave me a note — it’s always difficult to streamline such a process, and
I might have overseen some issues.
