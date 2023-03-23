# Parallel Execution

In addition to the parallel data processing by Spark itself, Flowman provides additional levels of parallelism in
order to speed up processing.


## Parallel instantiation of mappings

The first and simplest step to speed up processing in Flowman is to parallelize the instantiation of mappings. This
does not parallelize the transformation of records itself, but this feature parallelizes some aspects of the planning
phase of Apache Spark. For example reading information from the Hive meta store or listing files can be a slow and
lengthy process, which is mainly performed by the driver side leaving the worker cluster idle. By performing this
kind of work in parallel for multiple data sources, some latencies can be reduced.

In order to turn on this feature, you simply need to add the following configuration variable either to the
`config` section of your [project](../spec/project.md) or to the `config` section of your [namespace](../spec/namespace.md):

```yaml
config:
  - flowman.execution.mapping.parallelism=8
```


## Parallel execution of multiple targets

The next step for parallelizing work is executing multiple targets in parallel. This can speed up overall processing
time of a single job in case where individual jobs cannot make full use of a Spark cluster. In this case some idle 
resources can be used for working on additional execution targets.

In order to turn on this feature, you simply need to add the following configuration variable either to the
`config` section of your [project](../spec/project.md) or to the `config` section of your [namespace](../spec/namespace.md):

```yaml
config:
  # First you need to configure a different executor than the default one
  - flowman.execution.executor.class=com.dimajix.flowman.execution.ParallelExecutor
  # Then you need to specify the maximum number of targets to be executed in parallel
  - flowman.execution.executor.parallelism=4
```

Note that this feature is not compatible with the [manual scheduler](target-ordering.md), since Flowman needs to be
fully aware of data dependencies for correctly scheduling targets.


## Parallel execution of multiple job instances

Finally, Flowman is capable of executing multiple job instances in parallel. This feature requires a parametrized job
(for example, the processing day might be a good candidate). For example imaging the following job definition:

```yaml
jobs:
  main:
    # Define a single job parameter called "year"
    parameters:
      - name: year
        type: Integer
        default: 2013
    targets:
      - measurements
      - stations
      - aggregates
      - validate_stations_raw
    
```

Then the range of years between 2000 and 2020 can be sequentially executed via
```shell
flowexec -f my_project_directory job build main year:start=2000 year:end=2020
```
This will execute one year after the other. By adding the command line option `-j 4`, Flowman will process up to
four years in parallel:
```shell
flowexec -f my_project_directory job build main year:start=2000 year:end=2020 -j 4
```
