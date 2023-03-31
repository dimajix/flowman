# Lesson 4 - Parameters

This lesson is a refinement of the last lesson where the Flowman project was implemented to independently process
data partitions, where each partition represents one year. So far we used an environment variable to define the
data partition to process with Flowman. But there is a better more explicit alternative which also has some additional
benefits in terms of process management: You can specify mandatory execution parameters in a Flowman job.

## 1. What to Expect

### Objectives

* You will learn how to parametrize a Flowman job
* You will understand the difference between environment variables and job parameters
* You will learn the effect of using data partitions

You can find the full source code of this lesson [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/develop/lessons/04-parameters)

### Description
Again we will read in the raw measurement data, which contains many weather measurements per weather stations
(typically one measurement per weather station and per hour, but there might be more or less). We then will store
the extracted measurements in Parquet files again, which are well suited for any downstream analytical workload.

### Processing Steps
Again we will perform the following steps:
1. Read in weather measurements as simple text files from S3
2. Extract some measurement attributes via SQL functions
3. Write weather measurements as Parquet files into local file system


## 2. Implementation
The only modification to the last session is done in the environment definition and the job definition.

### 2.1 Configuration & Environment
We remove the environment variable `year` and go back to the initial simple version of the file `config/environment.yml`

```yaml
environment:
  - basedir=file:///tmp/weather
```

### 2.2 Job
We now add a execution parameter `year` to the job definition which replaces the environment variable which we just 
removed

```yaml
jobs:
  # Define the 'main' job, which implicitly is used whenever you build the whole project
  main:
    # Add a parameter for selecting the year to process. This will create an environment variable `$year` which
    # can be accessed from within other entities like mappings, relations, etc
    parameters:
      - name: year
        type: Integer
        default: 2013
    # List all targets which should be built as part of the `main` job
    targets:
      - measurements
```


## 3. Execution

With these two small changes in place we converted the normal environment variable `year` to a job parameter. This
parameter then can be specified when starting the execution as follows:

```shell
cd /home/flowman
flowexec -f lessons/04-parameters job build main year=2014 --force
```

This looks almost identical to the last lesson, only the `-D` is missing. But under the hood, more is going on. Flowman
will actually validate the parameter and if we didn't provide a default value in the job definition, Flowman would 
raise an error that this parameter was missing on the command line.

### 3.1 Range Processing
Sometimes it is required to process a whole range of parameters. This is also well supported by Flowman. For example
in order to process all years from 2013 until 2017 (exclusive), you simply need to execute

```shell
cd /home/flowman
flowexec -f lessons/04-parameters job build main year:start=2014 year:end=2017 --force
```

### 3.2 Parallel Range Processing
You can even process multiple years in parallel by adding the command line option `-j 2` for processing two years
in parallel.

```shell
cd /home/flowman
flowexec -f lessons/04-parameters job build main year:start=2014 year:end=2017 --force -j 2
```

Note that processing multiple data partitions in parallel does not always really speed up processing, since both 
executions are fighting for the same resources. But in cases of big initialization overhead (like listing directories
etc) in relation to small amounts of data, there can be a significant benefit of running two partitions in parallel.


## 4. Next Lesson

The next lesson will have a closer look at working with multiple targets within a single job.
