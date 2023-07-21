# Lesson 6 — Flowshell

Implementing complex data transformations using Flowmans declarative approach can reduce development efforts
significantly. But using the `flowexec` command for testing the syntax and logic during development turns out
to be quite inefficient. Therefore, Flowman comes with an interactive shell, with is a valuable tool for development
and debugging.

## 1. What to Expect

### Objectives

* You will learn how to use Flowman Shell to support development
* You will know the most important command of Flowman Shell
* You will understand job contexts and why they are required

We will use the source code of the last lesson, which is available [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/main/lessons/05-multiple-targets)

### Description

Since this chapter only describes an additional development tool for Flowman, we will simply reuse the project of the
last chapter. The project includes one job with multiple targets and relations.


## 2. Using the Flowman interactive shell

We can simply start the Flowman shell via the following command:
```shell
cd /home/flowman
flowshell -f lessons/05-multiple-targets
```
As you can see, we only specify the project to be loaded, but we leave out any specific command to be executed. You
will be presented with a Flowman logo and a prompt
```
[INFO] Using Flowman default system settings
[INFO] Reading namespace file /opt/flowman/conf/default-namespace.yml
[INFO] Reading plugin descriptor /opt/flowman/plugins/flowman-avro/plugin.yml
[INFO] Loading Plugin flowman-avro
[INFO] Reading plugin descriptor /opt/flowman/plugins/flowman-aws/plugin.yml
[INFO] Loading Plugin flowman-aws
[INFO] Reading plugin descriptor /opt/flowman/plugins/flowman-mssqlserver/plugin.yml
[INFO] Loading Plugin flowman-mssqlserver
[INFO] Reading project from file:/home/flowman/lessons/05-multiple-targets
[INFO] Reading all module files in directory file:/home/flowman/lessons/05-multiple-targets/mapping
[INFO] Reading all module files in directory file:/home/flowman/lessons/05-multiple-targets/job
[INFO] Reading all module files in directory file:/home/flowman/lessons/05-multiple-targets/model
[INFO] Reading all module files in directory file:/home/flowman/lessons/05-multiple-targets/config
[INFO] Reading all module files in directory file:/home/flowman/lessons/05-multiple-targets/target
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/target/aggregates.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/config/aws.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/mapping/aggregates.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/model/aggregates.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/model/measurements-raw.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/model/measurements.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/job/main.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/config/environment.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/model/stations-raw.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/model/stations.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/mapping/facts.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/target/measurements.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/target/stations.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/mapping/measurements.yml
[INFO] Reading module from file:/home/flowman/lessons/05-multiple-targets/mapping/stations.yml
[INFO] Loaded project 'weather' version 1.0

Welcome to
______  _
|  ___|| |
| |_   | |  ___ __      __ _ __ ___    __ _  _ __
|  _|  | | / _ \\ \ /\ / /| '_ ` _ \  / _` || '_ \
| |    | || (_) |\ V  V / | | | | | || (_| || | | |
\_|    |_| \___/  \_/\_/  |_| |_| |_| \__,_||_| |_|    1.0.0

Using Spark 3.3.2 and Hadoop 3.3.2 and Scala 2.12.15 (Java 11.0.15)

Type in 'help' for getting help
flowman:weather> 
```

The prompt understands all commands, which are also available in `flowexec`, plus some additional commands only
available in the Flowman interactive shell.


### 2.1 Inspecting the project

The shell provides some simple commands for inspecting the project.

#### Listing entities
For example, you can view a list of all jobs in the project with the following command:
```
flowman:weather> job list 
main
```
You can request a list of all targets, relations, mappings etc. with similar commands
```
flowman:weather> target list 
aggregates
measurements
stations
```
```
flowman:weather> relation list
aggregates
measurements
measurements_raw
stations
stations_raw
```
```
flowman:weather> mapping list 
aggregates
facts
measurements
measurements_extracted
measurements_joined
measurements_raw
stations
stations_raw
```

#### Inspecting entities
The shell also provides a couple of `inspect` commands, which show more detailed information for a single entity. For
example, the following command will show more detailed information for the relation `stations`:
```
flowman:weather> relation inspect stations
 Relation:
    name: stations
    kind: file
  Requires - CREATE:
    file:file:/home/flowman/lessons/05-multiple-targets/schema/stations.avsc
  Provides - CREATE:
    file:file:/tmp/weather/stations
  Requires - READ:
    file:file:/tmp/weather/stations
  Provides - READ:
  Requires - WRITE:
  Provides - WRITE:
    file:file:/tmp/weather/stations
```

Similar commands also exist for other entity types, for example for inspecting a mapping, you would type the
following:
```
flowman:weather> mapping inspect stations
Mapping:
    name: stations
    kind: relation
    inputs: 
    outputs: main
    cache: StorageLevel(1 replicas)
    broadcast: false
    checkpoint: false
  Requires:
    file:file:/tmp/weather/stations
```

#### Inspect schema

Diving into more details, you can also retrieve the data schema of a relation or mapping as follows:
```
flowman:weather> relation describe stations
root
 |-- usaf: string (nullable = false)
 |-- wban: string (nullable = false)
 |-- name: string (nullable = true)
 |-- country: string (nullable = true)
 |-- state: string (nullable = true)
 |-- icao: string (nullable = true)
 |-- latitude: float (nullable = true)
 |-- longitude: float (nullable = true)
 |-- elevation: float (nullable = true)
 |-- date_begin: date (nullable = true)
 |-- date_end: date (nullable = true)
```

#### Showing records

The Flowman shell can also peek into some entities, like mappings and relations and show some records:
```
flowman:weather> mapping show stations_raw
[INFO] Instantiating mapping 'weather/stations_raw' with outputs 'main' (broadcast=false, cache='None')
[INFO] Reading from relation 'stations_raw' with partitions () and filter ''
[INFO] Reading file relation 'weather/stations_raw' at 's3a://dimajix-training/data/weather/isd-history'  for partitions ()
[INFO] Loading schema from file file:/home/flowman/lessons/05-multiple-targets/schema/stations.avsc
+------+-----+----------+-------+-----+----+--------+---------+---------+----------+----------+
|  usaf| wban|      name|country|state|icao|latitude|longitude|elevation|date_begin|  date_end|
+------+-----+----------+-------+-----+----+--------+---------+---------+----------+----------+
|007018|99999|WXPOD 7018|   null| null|null|     0.0|      0.0|   7018.0|2011-03-09|2013-07-30|
|007026|99999|WXPOD 7026|     AF| null|null|     0.0|      0.0|   7026.0|2012-07-13|2017-08-22|
|007070|99999|WXPOD 7070|     AF| null|null|     0.0|      0.0|   7070.0|2014-09-23|2015-09-26|
|008260|99999| WXPOD8270|   null| null|null|     0.0|      0.0|      0.0|2005-01-01|2010-09-20|
|008268|99999| WXPOD8278|     AF| null|null|   32.95|   65.567|   1156.7|2010-05-19|2012-03-23|
|008307|99999|WXPOD 8318|     AF| null|null|     0.0|      0.0|   8318.0|2010-04-21|2010-04-21|
|008411|99999|      XM20|   null| null|null|    null|     null|     null|2016-02-17|2016-02-17|
|008414|99999|      XM18|   null| null|null|    null|     null|     null|2016-02-16|2016-02-17|
|008415|99999|      XM21|   null| null|null|    null|     null|     null|2016-02-17|2016-02-17|
|008418|99999|      XM24|   null| null|null|    null|     null|     null|2016-02-17|2016-02-17|
+------+-----+----------+-------+-----+----+--------+---------+---------+----------+----------+
only showing top 10 rows
```


### 2.2 Job context

When playing around with the shell, you might notice that the commands above do not work with all entities of the
project. In particular, they fail for entities, which access the job parameter `year`. This is quite natural, because
this variable is not defined globally, but only inside the main job. The same problem is true for all additional
environment variables defined within a job - they are not globally available.

In order to work with all entities, even if they rely on some parameters or variables defined within a job, you
can enter a *job context*, which will apply any job specific setting to the execution environment:
```
flowman:weather> job enter main year=2019
[INFO] Job argument year=2019
```
Now you also access entities, which (possibly indirectly) access the job parameter `year`. For example, the
relation `measuerements` can now also be inspected:
```
flowman:weather/main> relation inspect measurements
Relation:
    name: measurements
    kind: file
  Requires - CREATE:
    file:s3a://dimajix-training/data/weather/2019
  Provides - CREATE:
    file:file:/tmp/weather/measurements
  Requires - READ:
    file:file:/tmp/weather/measurements/year=2019
  Provides - READ:
  Requires - WRITE:
  Provides - WRITE:
    file:file:/tmp/weather/measurements/year=2019
```

Finally, you can also leave the context again:
```
flowman:weather/main> job leave
```

### 2.3 SQL

The Flowman shell also provides a simple way to execute arbitrary SQL commands, where tables refer to mappings.
For example, in order to count the number of records in the `stations_raw` mapping, you can simply execute the
following command:
```
flowman:weather> sql "SELECT COUNT(*) FROM stations_raw"
+--------+
|count(1)|
+--------+
|   29744|
+--------+
```

### 2.4 Executions

Of course, you can also execute jobs or individual targets inside the shell:

```
flowman:weather> job build main year=2019
[INFO] 
[INFO] -------------------------------------------------------------------------------------------------------------
[INFO] Executing phases 'VALIDATE','CREATE','BUILD' for job 'main' in project 'weather' (version 1.0) with isolation
[INFO] Job argument year=2019
[INFO] 
[INFO] -------------------------------------------------------------------------------------------------------------
...
```

## 3. Next Lesson
In the next lesson, we will talk about the lifecycle model of Flowman.
