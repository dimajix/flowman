# Performance Tuning

Processing performance always is an important topic for data transformation, and so is the case with Flowman. In order
to improve overall performance, there are different configurations, some of them being well known configuration
parameters for Apache Spark, while others are specific to Flowman.


## Spark Parameters

Since Flowman is based on Apache Spark, you can apply all the performance tuning strategies that apply to Spark. 
You can specify almost all settings either in the [`default-namespace.yml` file](../setup/config.md) or in any other
project file in a `config` section. The most important settings probably are as follows:

```yaml
config:
  # Use 8 CPU cores per Spark executor
  - spark.executor.cores=8
  # Allocate 54 GB RAM per Spark executor
  - spark.executor.memory=54g
  # Only keep up to 200 jobs in the Spark web UI
  - spark.ui.retainedJobs=200
  # Use 400 partitions in shuffle operations
  - spark.sql.shuffle.partitions=400
  # Number of executors to allocate
  - spark.executor.instances=2
  # Memory overhead as safety margin
  - spark.executor.memoryOverhead=1G
```

Often it is a good idea to make these properties easily configurable via system environment variables as follows:
```yaml
config:
 - spark.executor.cores=$System.getenv('SPARK_EXECUTOR_CORES', '8')
 - spark.executor.memory=$System.getenv('SPARK_EXECUTOR_MEMORY', '54g')
 - spark.ui.retainedJobs=$System.getenv('RETAINED_JOBS', 200)
 - spark.sql.shuffle.partitions=$System.getenv('SPARK_PARTITIONS', 400)
```


## Flowman Parameters

In addition to classical Spark tuning parameters, Flowman also offers some advanced functionality which may help to
cut down processing overhead cost by parallelizing target execution and mapping instantiation. This will not speed
up the processing itself, but it will help to hide some expensive Spark planning costs, which may involve querying
the Hive metastore or remote file systems, which are known to be slow.

```yaml
config:
  # Enable building multiple targets in parallel
  - flowman.execution.executor.class=com.dimajix.flowman.execution.ParallelExecutor
  # Build up to 4 targets in parallel
  - flowman.execution.executor.parallelism=4
  # Instantiate up to 16 mappings in parallel
  - flowman.execution.mapping.parallelism=16
```
