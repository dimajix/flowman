# Releases

Flowman releases are done on an irregular basis, whenever new features are available or important issues have been
fixed.

## Official Releases

You will find all official Flowman releases on [GitHub](https://github.com/dimajix/flowman/releases).


## Past Releases 

The following gives an (incomplete) list of past releases of the last 12 months. This will help you to spot breaking 
changes over time.

### Version 0.24.1 - 2022-04-28

* github-175: '--jobs' parameter starts way to many parallel jobs
* github-176: start-/end-date in report should not be the same
* github-177: Implement generic SQL schema check
* github-179: Update DeltaLake dependency to 1.2.1


### Version 0.24.0 - 2022-04-05

* github-168: Support optional filters in data quality checks
* github-169: Support sub-queries in filter conditions
* github-171: Parallelize loading of project files
* github-172: Update CDP7 profile to the latest patch level
* github-153: Use non-privileged user in Docker image
* github-174: Provide application for generating YAML schema

#### Breaking changes

We take backward compatibility very seriously. But sometimes a breaking change is needed to clean up code and to
enable new features. This release contains some breaking changes, which are annoying but simple to fix.
In order to avoid YAML schema inconsistencies, some entities needed to be renamed, as described in the following
table:

| category | old kind     | new kind             |
|----------|--------------|----------------------|
| mapping  | const        | values               |
| mapping  | empty        | null                 |
| mapping  | read         | relation             |
| mapping  | readRelation | relation             |
| mapping  | readStream   | stream               |
| relation | const        | values               |
| relation | empty        | null                 |
| relation | jdbc         | jdbcTable, jdbcQuery |
| relation | table        | hiveTable            |
| relation | view         | hiveView             |
| schema   | embedded     | inline               |


### Version 0.23.1 - 2022-03-28

* github-154: Fix failing migration when PK requires change due to data type
* github-156: Recreate indexes when data type of column changes
* github-155: Project level configs are used outside job
* github-157: Fix UPSERT operations for SQL Server
* github-158: Improve non-nullability of primary key column
* github-160: Use sensible defaults for default documenter
* github-161: Improve schema caching during execution
* github-162: ExpressionColumnCheck does not work when results contain NULL values
* github-163: Implement new column length quality check


### Version 0.23.0 - 2022-03-18

* github-148: Support staging table for all JDBC relations
* github-120: Use staging tables for UPSERT and MERGE operations in JDBC relations
* github-147: Add support for PostgreSQL
* github-151: Implement column level lineage in documentation
* github-121: Correctly apply documentation, before/after and other common attributes to templates
* github-152: Implement new 'cast' mapping


### Version 0.22.0 - 2022-03-01

* Add new `sqlserver` relation
* Implement new documentation subsystem
* Change default build to Spark 3.2.1 and Hadoop 3.3.1
* Add new `drop` target for removing tables
* Speed up project loading by reusing Jackson mapper
* Implement new `jdbc` metric sink
* Implement schema cache in Executor to speed up documentation and similar tasks
* Add new config variables `flowman.execution.mapping.schemaCache` and `flowman.execution.relation.schemaCache`
* Add new config variable `flowman.default.target.verifyPolicy` to ignore empty tables during VERIFY phase
* Implement initial support for indexes in JDBC relations



### Version 0.21.1 - 2022-01-28

* `flowexec` now returns different exit codes depending on the processing result


### Version 0.21.0 - 2022-01-26

* Fix wrong dependencies in Swagger plugin
* Implement basic schema inference for local CSV files
* Implement new `stack` mapping
* Improve error messages of local CSV parser


### Version 0.20.1 - 2022-01-06

* Implement detection of dependencies introduced by schema


### Version 0.20.0 - 2022-01-05

* Fix detection of Derby metastore to truncate comment lengths.
* Add new config variable `flowman.default.relation.input.columnMismatchPolicy` (default is `IGNORE`)
* Add new config variable `flowman.default.relation.input.typeMismatchPolicy` (default is `IGNORE`)
* Add new config variable `flowman.default.relation.output.columnMismatchPolicy` (default is `ADD_REMOVE_COLUMNS`)
* Add new config variable `flowman.default.relation.output.typeMismatchPolicy` (default is `CAST_ALWAYS`)
* Improve handling of `_SUCCESS` files for detecting (non-)dirty directories
* Implement new `merge` target
* Implement merge operation for Delta relations
* Implement merge operation for JDBC relations (only for some databases, i.e. MS SQL)
* Add new config variable `flowman.execution.target.useHistory` (default is `false`)
* Change the semantics of config variable `flowman.execution.target.forceDirty` (default is `false`)
* Add new `-d` / `--dirty` option for explicitly marking individual targets as dirty


### Version 0.19.0 - 2021-12-13

* Add build profile for Hadoop 3.3
* Add build profile for Spark 3.2
* Allow SQL expressions as dimensions in `aggregate` mapping
* Update Hive views when the resulting schema would change
* Add new `mapping cache` command to FlowShell
* Support embedded connection definitions
* Much improved Flowman History Server
* Fix wrong metric names with TemplateTarget
* Implement more `template` types for `connection`, `schema`, `dataset`, `assertion` and `measure`
* Implement new `measure` target for creating custom metrics for measuring data quality
* Add new config option `flowman.execution.mapping.parallelism`


### Version 0.18.0 - 2021-10-13

* Improve automatic schema migration for Hive and JDBC relations
* Improve support of `CHAR(n)` and `VARCHAR(n)` types. Those types will now be propagated to Hive with newer Spark versions
* Support writing to dynamic partitions for file relations, Hive tables, JDBC relations and Delta tables
* Fix the name of some config variables (floman.* => flowman.*)
* Added new config variables `flowman.default.relation.migrationPolicy` and `flowman.default.relation.migrationStrategy`
* Add plugin for supporting DeltaLake (https://delta.io), which provides `deltaTable` and `deltaFile` relation types
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
* Add new option "-j <n>" to allow running multiple tests in parallel
* Add new `uniqueKey` assertion
* Add new `schema` assertion
* Update Swagger libraries for `swagger` schema
* Implement new `openapi` plugin to support OpenAPI 3.0 schemas
* Add new `readHive` mapping
* Add new `simpleReport` and `report` hook
* Implement new templates


### Version 0.17.0 - 2021-06-02

* New Flowman Kernel and Flowman Studio application prototypes
* New ParallelExecutor
* Fix before/after dependencies in `count` target
* Default build is now Spark 3.1 + Hadoop 3.2
* Remove build profiles for Spark 2.3 and CDH 5.15
* Add MS SQL Server plugin containing JDBC driver
* Speed up file listing for `file` relations
* Use Spark JobGroups
* Better support running Flowman on Windows with appropriate batch scripts


### Version 0.16.0 - 2021-04-23

* Add logo to Flowman Shell
* Fix name of config option `flowman.execution.executor.class`
* Add new `groupedAggregate` mapping
* Reimplement target ordering, configurable via `flowman.execution.scheduler.class`
* Implement new assertions `columns` and `expression`


### Version 0.15.0 - 2021-03-23

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

