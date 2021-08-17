# Version 0.18.0

* Improve automatic schema migration for Hive and JDBC relations
* Improve support of CHAR(n) and VARCHAR(n) types. Those types will now be propagates to Hive with newer Spark versions
* Support writing to dynamic partitions for file relations, Hive tables, JDBC relations and Delta tables
* Fix the name of some config variables (floman.* => flowman.*)
* Added new config variables `flowman.default.relation.migrationPolicy` and `flowman.default.relation.migrationStrategy`
* Add plugin for supporting DeltaLake (https://delta.io)
* Fix non-deterministic column order in `schema` mapping, `values` mapping and `values` relation 
* Mark Hive dependencies has 'provided', which reduces the size of dist packages
* Significantly reduce size of AWS dependencies in AWS plugin
* Add new build profile for Cloudera CDP-7.1
* Improve Spark configuration of `LocalSparkSession` and `TestRunner`  
* Update Spark 3.0 build profile to Spark 3.0.3
* Upgrade Impala JDBC driver from 2.6.17.1020 to 2.6.23.1028
* Upgrade MySQL JDBC driver from 8.0.20 to 8.0.25  
* Upgrade MariaDB JDBC driver from 2.2.4 to 2.7.3
* Upgrade several Maven plugins to latest versions
* Add new config option `flowman.workaround.analyze_partition` to workaround CDP 7.1 issues
* Fix migrating Hive views to tables and vice-versa
* Add new option "-j <n>" to allow running multiple job instances in parallel


# Version 0.17.1 - 2021-06-18

* Bump CDH version to 6.3.4
* Fix scope of some dependencies
* Update Spark to 3.1.2
* Add new `values` relation


# Version 0.17.0 - 2021-06-02

* New Flowman Kernel and Flowman Studio application prototypes
* New ParallelExecutor
* Fix before/after dependencies in `count` target
* Default build is now Spark 3.1 + Hadoop 3.2   
* Remove build profiles for Spark 2.3 and CDH 5.15
* Add MS SQL Server plugin containing JDBC driver
* Speed up file listing for `file` relations  
* Use Spark JobGroups
* Better support running Flowman on Windows with appropriate batch scripts


# Version 0.16.0 - 2021-04-23

* Add logo to Flowman Shell
* Fix name of config option `flowman.execution.executor.class`
* Add new `groupedAggregate` mapping
* Reimplement target ordering, configurable via `flowman.execution.scheduler.class`
* Implement new assertions `columns` and `expression`


# Version 0.15.0 - 2021-03-23

* New configuration variable `floman.default.target.rebalance`
* New configuration variable `floman.default.target.parallelism`
* Changed behaviour: The `mergeFile` target now does not assume any more that the `target` is local. If you already
  use `mergeFiles` with a local file, you need to prefix the target file name with `file://`.
* Add new `-t` argument for selectively building a subset of targets
* Remove example-plugin
* Add quickstart guide  
* Add new "flowman-parent" BOM for projects using Flowman
* Move `com.dimajix.flowman.annotations` package to `com.dimajix.flowman.spec.annotations`
* Add new log redaction
* Integrate Scala scode coverage analysis
* `assemble` will fail when trying to use non-existing columns
* Move `swagger` and `json` schema support into separate plugins
* Change default build to Spark 3.0 and Hadoop 3.2
* Update Spark to 3.0.2
* Rename class `Executor` to `Execution` - watch your plugins!
* Implement new configurable `Executor` class for executing build targets.
* Add build profile for Spark 3.1.x
* Update ScalaTest to 3.2.5 - watch your unittests for changed ScalaTest API!
* Add new `case` mapping
* Add new `--dry-run` command line option
* Add new `mock` and `null` mapping types
* Add new `mock` relation
* Add new `values` mapping
* Add new `values` dataset  
* Implement new testing capabilities  
* Rename `update` mapping to `upsert` mapping, which better describes its functionality 
* Introduce new `VALIDATE` phase, which is executed even before `CREATE` phase
* Implement new `validate` and `verify` targets
* Implement new `deptree` command in Flowman shell


# Version 0.14.2 - 2020-10-12

* Upgrade to Spark 2.4.7 and Spark 3.0.1
* Clean up dependencies
* Disable build of Docker image
* Update examples


# Version 0.14.1 - 2020-09-28

* Fix dropping of partitions which could cause issues on CDH6 


# Version 0.14.0 - 2020-09-10

* Fix AWS plugin for Hadoop 3.x
* Improve setup of logging
* Shade Velocity for better interoperability with Spark 3
* Add new web hook facility in namespaces and jobs
* Existing targets will not be overwritten anymore by default. Either use the `--force` command line option, or set 
the configuration property `flowman.execution.target.forceDirty` to `true` for the old behaviour.
* Add new command line option `--keep-going`
* Implement new `com.dimajix.spark.io.DeferredFileCommitProtocol` which can be used by setting the Spark configuration
parameter `spark.sql.sources.commitProtocolClass`
* Add new `flowshell` application


# Version 0.13.1 - 2020-07-14

* Code improvements
* Do not implicitly set SPARK_MASTER in configuration
* Add support for CDH6
* Add support for Spark 3.0
* Improve support for Hadoop 3.x


# Version 0.13.0 - 2020-04-21

* Refactor Maven module structure
* Implement new Scala DSL for creating projects
* Fix ordering bug in target execution
* Merge 'migrate' phase into 'create' phase
* Rename 'input' field to 'mapping' in most targets
* Lots of minor code improvements


# Version 0.12.2 - 2020-04-06

* Fix type coercion of DecimalTypes


# Version 0.12.1 - 2020-01-15

* Improve support for Swagger Schema
* Fix infinite loop in recursiveSql


# Version 0.12.0 - 2020-01-09

* Add new RecursiveSqlMapping
* Refactor 'describe' method of mappings
* Fix TemplateRelation to return correct partitions and fields
* Add 'filter' attribute to many mappings


# Version 0.11.6 - 2019-12-17

* Improve build dependency management with DataSets


# Version 0.11.5 - 2019-12-16

* Update to newest Swagger V2 parser
* Workaround for bug in Swagger parser for enums
* Tidy up logging


# Version 0.11.4 - 2019-12-11

* Remove HDFS directories when dropping Hive table partitions


# Version 0.11.3 - 2019-12-06

* Improve migrations of HiveUnionTable
* Improve schema support in 'copy' target 
 

# Version 0.11.2 - 2019-12-02

* Add new 'earliest' mapping


# Version 0.11.1 - 2019-11-29

* Improve Hive compatibility of SQL generator for UNION statements


# Version 0.11.0 - 2019-11-28

* Add support for Spark 3.0 preview
* Remove HBase plugin
* Add optional 'filter' to 'readRelation' mapping
* Improve Hive compatibility of SQL generator for ORDER BY statements
* Fix target table search in Hive Union Table


# Version 0.10.2 - 2019-11-18

* Improve Impala catalog support


# Version 0.10.1 - 2019-11-14

* Add 'error' output to 'extractJson' mapping


# Version 0.10.0 - 2019-11-05

* Add new 'hiveUnionTable' relation
* Add new 'union' schema
* Support nested columns in deduplication
* Support nested Hive VIEWs
* Support Spark 2.4.4


# Version 0.9.1 - 2019-10-10

* Fix wrong Spark application name if configured via Spark config


# Version 0.9.0 - 2019-10-08

* Complete overhaul of job execution. No tasks any more
* Improve Swagger schema support


# Version 0.8.3 - 2019-09-16

* Add configuration option for column insert position of 'historize' mapping


# Version 0.8.2 - 2019-08-29

* Add optional filter condition to 'latest' mapping


# Version 0.8.1 - 2019-08-29

* Improve generation of SQL code containing window functions


# Version 0.8.0 - 2019-08-28

* Add new metric system
* Add Hive view generation from mappings
* Support for Hadoop 3.1 and 3.2 (without Hive)
* Add 'historize' mapping


# Version 0.7.1 - 2019-07-22

* Fix build with CDH-5.15 profile


# Version 0.7.0 - 2019-07-22

* Implement initial REST server
* Implement initial prototype of UI
* Implement new datasets for new tasks (copy/compare/...)


# Version 0.6.5 - 

* Add support for checkpoint directory


# Verison 0.6.4 - 2019-06-20

* Implement column renaming in projections


# Verison 0.6.3 - 2019-06-17

* CopyRelationTask also performs projection


# Version 0.6.2 - 2019-06-12

* "explode" mapping supports simple data types


# Version 0.6.1 - 2019-06-11

* Fix NPE in ShowRelationTask


# Version 0.6.0 - 2019-06-11

* Add multiple relations to `showRelation` task
* github-33: Add new `unit` mapping
* github-34: Fix NPE in Swagger schema reader
* github-36: Add new `explode` mapping

# Version 0.5.0 - 2019-05-28

* github-32: Improve handling of nullable structs
* github-30: Refactoring of whole specification handling
* github-30: Add new 'template' mapping
* Add new `flatten` entry in assembler
* Implement new `flatten` mapping
* github-31: Fix handling of local project definition in flowexec


# Version 0.4.1 - 2019-05-11

* Add parameters to "job run" CLI
* Fix error handling of failing "build" and "clean" tasks


# Version 0.4.0 - 2019-05-08

* Add support for Spark 2.4.2
* Add new assemble mapping
* Add new conform mapping


# Version 0.3.0 - 2019-03-20

* Add null format for Spark
* Add deployment


# Version 0.1.2 - 2018-11-05

* Update to Spark 2.3.2
* Small fixes


# Version 0.1.1 - 2018-09-14

Small fixes


# Version 0.1.0 - 2018-07-20

Initial release
