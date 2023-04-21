# Flowman Quickstart Guide

This quickstart guide will walk you through the first steps working with Flowman. We will be using Flowman in Docker
to save setup and configuration of Spark and Flowman.

Of course running Flowman inside Docker will probably prevent a proper integration with any existing Hadoop environment
like Cloudera or EMR. This requires a local installation of Flowman on your Linux (or Windows) machine, which is described
in more detail in [installation guide](setup/installation.md).


## 1. Start Docker image

Of course, you need a working Docker installation, which should be quite easy. Then you can start a Docker image
containing Flowman via the following command:

```shell
docker run --rm -ti dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3 bash   
```

Note that this simply starts a bash shell, but Flowman is only away some fingertips. 

### Mounting volumes

You probably might want to mount some local directory into the Docker container running Flowman. For example you
may want to make local data accessible to Flowman or you may simply want to execute some Flowman project stored
on your local machine. This can be easily achieved as follows:

```shell
docker run --rm -ti -v /home/kaya/flowman/example/weather:/home/flowman/local dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3 bash   
```

This will mount the local directory `/home/kaya/flowman/example/weather` on your host computer into the Docker
container at `/home/flowman/local`.


## 2. Flowman Shell

The example data is stored in a publicly accessible S3 bucket. Since the data is publicly available and the project is
configured to use anonymous AWS authentication, you do not need to provide your AWS credentials (you even do not
even need to have an account on AWS)

### Start interactive Flowman shell

We start Flowman by running the interactive Flowman shell. While this is not the tool that would be used in automatic
batch processing (`flowexec` is the right tool for that scenario), it gives us a good idea how ETL projects in Flowman
are organized.

```shell
flowshell -f examples/weather
```

### Inspecting Relations

Now we can inspect some of the relations defined in the project. First we list all relations 
```
flowman:weather> relation list
```

Now we can peek inside the relations `stations_raw` and `measurements_raw`. Since the second relation is partitioned
by years, we explicitly specify the year via the option `-p year=2011`
```
flowman:weather> relation show stations_raw
flowman:weather> relation show measurements_raw -p year=2011
```

### Running a Job

Now we want to execute the projects main job. Again the job is parametrized by year, so we need to specify the year
that we'd like to process.
```
flowman:weather> job build main year=2011
```

### Inspecting Mappings

Now we'd like to inspect some of the mappings which have been used during job execution. Since some mappings depend
on job-specific variables, we need to create a *job context*, which can be done by `job enter <job-name> <job-args>`
as follows:
```
flowman:weather> job enter main year=2011
```
Note how the prompt has changed and will now include the job name. Now we can inspect some mappings:
```
flowman:weather/main> mapping list
flowman:weather/main> mapping show measurements_raw
flowman:weather/main> mapping show measurements_extracted
flowman:weather/main> mapping show stations_raw
```
Finally we'd like to leave the job context again.
```
flowman:weather/main> job leave
```


### Inspecting Results

The job execution has written its results into some relations again. We can now inspect them again
```
flowman:weather> relation show stations
flowman:weather> relation show measurements
flowman:weather> relation show aggregates -p year=2011
```

### History

Flowman also provides an execution history. In the trivial deployment, this information is stored locally in a
Derby database, but other databases like MySQL, MariaDB etc. are also supported.
```
flowman:weather> history job search
flowman:weather> history target search -P weather
```


### Generating Documentation

Flowman cannot only execute all the data transformations specified in the example project, it can also generate
a documentation, which will be stored as an html file
```
flowman:weather> documentation generate
```
This will create a file in the directory `examples/weather/generated-documentation/project.html` which can be viewed
by any web browser of your choice.


### Quitting

Finally, we quit the Flowman shell via the `quit` command.
```
flowman:weather> quit
```


## 3. Flowman Batch Execution

So far we have only used the Flowman shell for interactive work with projects. Actually, the shell was developed as a
second step to help to analyze problems and debugging data flows. The primary command for working with Flowman projects 
is `flowexec` which is used for non-interactive batch execution, for example within cron-jobs.

It shares a lot of code with the Flowman shell, so the commands are often exactly the same. The main difference is 
that with `flowexec` you specify the commands on the command line while `flowshell` provides its own prompt.

For example for running the “build” lifecycle of the weather project for the year 2014, you only need to run:
```shell
flowexec -f examples/weather job build main year=2014
```


## 4. Congratulations!

A very special *Thank You!* goes to all of you who try to follow the example hands-on on your local machine. If you have 
problems with following the example, please leave me a note — it’s always difficult to streamline such a process, and 
I might have overseen some issues.
