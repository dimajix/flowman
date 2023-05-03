# Flowman Docker Images

Flowman is a *data build tool* based on [Apache Spark](https://spark.apache.org) that simplifies the act of
implementing data transformation logic as part of complex data pipelines. Flowman follows a strict "everything-as-code"
approach, where the whole transformation logic is specified in purely declarative YAML files.
These describe all details of the data sources, sinks and data transformations. This is much simpler and efficient
than writing Spark jobs in Scala or Python. Flowman will take care of all the technical details of a correct and robust
implementation and the developers can concentrate on the data transformations themselves.

In addition to writing and executing data transformations, Flowman can also be used for managing physical data models,
i.e. Hive or SQL tables. Flowman can create such tables from a specification with the correct schema and also
automatically perform migrations. This helps to
keep all aspects (like transformations and schema information) in a single place managed by a single tool.


## 1. Starting Flowman container

We publish Flowman Docker images on [Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman),
which are good enough for local work. You can easily start a Flowman session in Docker as follows:

```shell
docker run --rm -ti dimajix/flowman:1.0.0-oss-spark3.3-hadoop3.3 bash
```

### Mounting projects

By using Docker volumes, you can easily mount a Flowman project into the Docker container, for example

```shell
docker run --rm -ti --mount type=bind,source=$(pwd)/my_project,target=/home/flowman/my_project dimajix/flowman:1.0.0-oss-spark3.3-hadoop3.3 bash
```
The command above will start a Docker container running Flowman, and the local subdirectory `my_project` within the
current working directory is mounted into the container at `/home/flowman/my_project`. Then you open your project
within the [Flowman Shell](../cli/flowshell/index.md) via
```shell
flowshell -f my_project
```
This way you can easily work with a normal code editor to modify the project definition in your normal file system.
Any change is immediately visible inside the Docker container, so you only need to perform a `project reload` within
the Flowman Shell to load any modifications to your project.


## 2. Flowman Shell

The example data is stored in a publicly accessible S3 bucket. Since the data is publicly available and the project is
configured to use anonymous AWS authentication, you do not need to provide your AWS credentials (you even do not
even need to have an account on AWS)

### Start interactive Flowman shell

We start Flowman by running the interactive Flowman shell. While this is not the tool that would be used in automatic
batch processing (`flowexec` is the right tool for that scenario), it gives us a good idea how ETL projects in Flowman
are organized.

```shell
cd flowman
bin/flowshell -f examples/weather
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
Derby database, but other databases like MySQL, MariaDB etc are also supported.
```
flowman:weather> history job search
flowman:weather> history target search -P weather
```


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
bin/flowexec -f examples/weather job build main year=2014
```


## 4. Congratulations!

A very special *Thank You!* goes to all of you who try to follow the example hands-on on your local machine. If you have
problems with following the example, please leave me a note — it’s always difficult to streamline such a process, and
I might have overseen some issues.
