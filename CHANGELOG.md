# Version Policy

Starting with version 1.0.0, Flowman uses a semantic version schema. This means that Flowman uses a three-digit 
version number *major*.*minor*.*bugfix*. The meaning of the components is as follows:

* The *major* number is increased in two scenarios: Either Flowman has implemented a big new feature set, or some
changes lead to incompatibility with previous versions (breaking changes).
* The *minor* number is increased to indicating new features or slightly changed behaviour.
* The *bugfix* number is increased for bugfixes. Otherwise, the behaviour and feature set of Flowman remain unchanged.

Generally, you can expect Flowman to be downwards compatible with older releases within the same major version. Some
minor changes or bugfixes may change the behavior of some features, which have been changed intentionally (for example,
even a small bugfix intentionally changes the behaviour).

Breaking changes will be documented in this changelog file for each version.


# Changelog

## Version 1.0.0

* github-314: Move avro related functionality into separate plugin
* github-307: Describe Flowmans security policy in SECURITY.md
* github-315: Create build profile for CDP 7.1 with Spark 3.3
* github-317: Perform retry on failing JDBC commands
* github-318: Support mappings from different projects and with non-standard outputs in SQL
* github-140: Strictly check imports
* github-316: Beautify README.md
* github-310: Explain versioning policy in CHANGELOG.md
* github-313: Improve example for "observe" mapping
* github-319: Support Oracle for History Server 
* github-320: Do not fall back to "inline" schema when no kind is specified
* github-321: Properly support lower case / upper case table names in Oracle
* github-309: Automate integration tests
* github-322: Remove flowman-client
* github-324: Log environment variables for imported projects
* github-329: Create Kernel API
* github-330: Implement Kernel Server
* github-331: Implement Kernel Client
* github-332: Build Flowman Shell on top of kernel Client/Server
* github-334: Create standalone Flowman Kernel application
* github-338: Update Spark to 3.3.2
* github-333: Forward Logs from Kernel to Client
* github-339: Set Copyright to "The Flowman Authors"
* github-345: Loading an embedded schema inside a jar file should not throw an exception
* github-346: Create build profile for Databricks
* github-343: Log all client requests in kernel
* github-342: Automatically close session when client disconnects from kernel
* github-351: Failing execution listener instantiation should not fail a build
* github-347: Exclude AWS SDK for Databricks and EMR build profiles
* github-352: Spark sessions should not contain duplicate jars from different plugins
* github-353: Successful runs should not use System.exit(0)

### Breaking changes

This version introduces some (minor) breaking changes:
* All Avro related functionality is now moved into the new "flowman-avro" plugin. If you rely on such functionality,
you explicitly need to include the plugin in the `default-namesapce.yml` file.
* Imports are now strictly checked. This means when you cross-reference some entity in your project which is provided
by a different Flowman project, you now need to explicitly import the project in the `project.yml`
* The `kind` for schema definitions is now a mandatory attribute, Flowman will not fall back to a `inline` schema anymore. 


## Version 0.30.0 - 2023-01-03

* github-278: Parallelize execution of data quality checks. This also introduces a new configuration property
  `flowman.execution.check.parallelism` (default `1`)
* github-282: Improve implementation for counting records
* github-288: Support reading local CSV files from fatjar
* github-290: Simplify specifying project name in fatjar
* github-291: Simplify create/destroy Relation interface
* github-292: Upgrade AWS EMR to 6.9
* github-289: Color log output via log4j configuration (requires log4j 2.x)
* Bump postgresql from 42.4.1 to 42.4.3 in /flowman-plugins/postgresql
* Bump loader-utils from 1.4.0 to 1.4.2
* Bump json5 from 2.2.1 to 2.2.3
* github-293: [BUG] Fatal exceptions in parallel mapping instantiation cause deadlock
* github-273: Support projects contained in (fat) jar files 
* github-294: [BUG] Parallel execution should not execute more targets after errors
* github-295: Create build profile for CDP 7.1 with Spark 3.2
* github-296: Update npm dependencies (vuetify & co)
* github-297: Parametrize when to execute a specific phase
* github-299: Move migrationPolicy and migrationStrategy from target into relation
* github-115: Implement additional build policy in relation target for forcing dirty. This also introduces a new
  configuration property `flowman.default.target.buildPolicy` (default `COMPAT`).
* github-298: Support fine-grained control when to execute each target of a job
* github-300: Implement new 'observe' mapping
* github-301: Upgrade Spark to 3.2.3
* github-302: Upgrade DeltaLake to 2.2.0
* github-303: Use multi-stage build for Docker image
* github-304: Upgrade Cloudera profile to CDP 7.1.8
* github-312: Fix build with Spark 2.4 and Maven 3.8

This version is fully backward compatible until and including version 0.27.0.


## Version 0.29.0 - 2022-11-08

* github-260: Remove hive-storage-api from several plugins and lib
* github-261: Add descriptions to all pom.xml
* github-262: Verification of "relation" targets should only check existence
* github-263: Add filter condition to data quality checks in documentation
* github-265: Make JDBC dialects pluggable
* github-264: Provide "jars" for all plugins
* github-267: Add new flowman-spark-dependencies module to simplify dependency management
* github-269: Implement new 'iterativeSql' mapping
* github-270: Upgrade Spark to 3.3.1
* github-271: Upgrade Delta to 2.1.1
* github-272: Create build profile for AWS EMR 6.8.0
* github-273: Refactor file abstraction
* github-274: Print Flowman configuration to console

This version is fully backward compatible until and including version 0.27.0.


## Version 0.28.0 - 2022-10-07

* Improve support for MariaDB / MySQL as data sinks
* github-245: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-studio-ui
* github-246: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-server-ui
* github-247: Automatically generate YAML schemas as part of build process
* github-248: Bump scss-tokenizer and node-sass in /flowman-server-ui
* github-249: Add new options -X and -XX to increase logging
* github-251: Support for log4j2 Configuration
* github-252: Move `sftp` target into separate plugin
* github-253: SQL Server relation should support explicit staging table
* github-254: Use DATETIME2 for timestamps in MS SQL Server
* github-256: Provide Maven archetype for simple Flowman projects
* github-258: Support clustered indexes in MS SQL Server

This version is fully backward compatible until and including version 0.27.0.


## Version 0.27.0 - 2022-09-09

* github-232: [BUG] Column descriptions should be propagated in UNIONs
* github-233: [BUG] Missing Hadoop dependencies for S3, Delta, etc
* github-235: Implement new `rest` hook with fine control
* github-229: A build target should not fail if Impala "COMPUTE STATS" fails
* github-236: 'copy' target should not apply output schema
* github-237: `jdbcQuery` relation should use fields "sql" and "file" instead of "query"
* github-239: Allow optional SQL statement for creating jdbcTable
* github-238: Implement new `jdbcCommand` target
* github-240: [BUG] Data quality checks in documentation should not fail on NULL values
* github-241: Throw an error on duplicate entity definitions
* github-220: Upgrade Delta-Lake to 2.0 / 2.1
* github-242: Switch to Spark 3.3 as default
* github-243: Use alternative Spark MS SQL Connector for Spark 3.3
* github-244: Generate project HTML documentation with optional external CSS file
* github-228: Change default of config `flowman.default.relation.input.charVarcharPolicy` to `IGNORE`

This version breaks full backward compatibility with older versions! See more details below.

### Breaking changes

In order to provide more flexibility and to increase consistency, the properties of `jdbcQuery` have changed. Before
this version, you needed to specify the SQL query in the `query` field. Now you have to specify the SQL query either
in the `sql` field or provide an external file and provide the file name in the `file` field. This new syntax is more 
consistent with `jdbcView` and `hiveView` relations.


## Version 0.26.1 - 2022-08-03

* github-226: Upgrade to Spark 3.2.2
* github-227: [BUG] Flowman should not fail with field names containing "-", "/" etc
* github-228: Padding and truncation of CHAR(n)/VARCHAR(n) should be configurable

This version is fully backward compatible until and including version 0.26.0.


## Version 0.26.0 - 2022-07-27

* github-202: Add support for Spark 3.3
* github-203: [BUG] Resource dependencies for Hive should be case-insensitive
* github-204: [BUG] Detect indirect dependencies in a chain of Hive views
* github-207: [BUG] Build should not directly fail if inferring dirty status fails
* github-209: [BUG] HiveViews should not trigger cascaded refresh during CREATE phase even when nothing is changed
* github-211: Implement new hiveQuery relation
* github-210: [BUG] HiveTables should be migrated if partition columns change
* github-208: Implement JDBC hook for database based semaphores
* github-212: [BUG] Hive views should not be migrated in RELAXED mode if only comments have changed
* github-214: Update ImpalaJDBC driver to 2.6.26.1031
* github-144: Support changing primary key for JDBC relations
* github-216: [BUG] Floats should be represented as FLOAT and not REAL in MySQL/MariaDB
* github-217: Support collations for creating/migrating JDBC tables
* github-218: [BUG] Postgres dialect should be used for Postgres JDBC URLs
* github-219: [BUG] SchemaMapping should retain incoming comments
* github-215: Support COLUMN STORE INDEX for MS SQL Server
* github-182: Support column descriptions in JDBC relations (SQL Server / Azure SQL)
* github-224: Support column descriptions for MariaDB / MySQL databases
* github-223: Support column descriptions for Postgres database
* github-205: Initial support Oracle DB via JDBC
* github-225: [BUG] Staging schema should not have comments

This version breaks full backward compatibility with older versions! See more details below.

### Breaking changes

We take backward compatibility very seriously. But sometimes a breaking change is needed to clean up code and to
enable new features. This release contains some breaking changes, which are annoying but simple to fix.
In order to respect `null` as keyword in YAML with a special semantics, some entities needed to be renamed, as 
described in the following table:

| category | old kind | new kind |
|----------|----------|----------|
| mapping  | null     | empty    |
| relation | null     | empty    |
| target   | null     | empty    |
| store    | null     | none     |
| history  | null     | none     |


## Version 0.25.1 - 2022-06-15

* github-195: [BUG] Metric "target_records" is not reset correctly after an execution phase is finished
* github-197: [BUG] Impala REFRESH METADATA should not fail when dropping views

This version is fully backward compatible until and including version 0.24.0.


## Version 0.25.0 - 2022-05-31

* github-184: Only read in *.yml / *.yaml files in module loader
* github-183: Support storing SQL in external file in `hiveView`
* github-185: Missing _SUCCESS file when writing to dynamic partitions
* github-186: Support output mode `OVERWRITE_DYNAMIC` for Delta relation
* github-149: Support creating views in JDBC with new `jdbcView` relation
* github-190: Replace logo in documentation
* github-188: Log detailed timing information when writing to JDBC relation
* github-191: Add user provided description to quality checks
* github-192: Provide example queries for JDBC metric sink

This version is fully backward compatible until and including version 0.24.0.


## Version 0.24.1 - 2022-04-28

* github-175: '--jobs' parameter starts way to many parallel jobs
* github-176: start-/end-date in report should not be the same
* github-177: Implement generic SQL schema check
* github-179: Update DeltaLake dependency to 1.2.1

This version is fully backward compatible until and including version 0.24.0.


## Version 0.24.0 - 2022-04-05

* github-168: Support optional filters in data quality checks
* github-169: Support sub-queries in filter conditions
* github-171: Parallelize loading of project files
* github-172: Update CDP7 profile to the latest patch level
* github-153: Use non-privileged user in Docker image
* github-174: Provide application for generating YAML schema

This version breaks full backward compatibility with older versions! See more details below.

### Breaking changes

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

 
## Version 0.23.1 - 2022-03-28

* github-154: Fix failing migration when PK requires change due to data type
* github-156: Recreate indexes when data type of column changes
* github-155: Project level configs are used outside job
* github-157: Fix UPSERT operations for SQL Server
* github-158: Improve non-nullability of primary key column
* github-160: Use sensible defaults for default documenter
* github-161: Improve schema caching during execution
* github-162: ExpressionColumnCheck does not work when results contain NULL values
* github-163: Implement new column length quality check

This version is fully backward compatible until and including version 0.18.0.


## Version 0.23.0 - 2022-03-18

* github-148: Support staging table for all JDBC relations
* github-120: Use staging tables for UPSERT and MERGE operations in JDBC relations
* github-147: Add support for PostgreSQL
* github-151: Implement column level lineage in documentation
* github-121: Correctly apply documentation, before/after and other common attributes to templates
* github-152: Implement new 'cast' mapping

This version is fully backward compatible until and including version 0.18.0.


## Version 0.22.0 - 2022-03-01

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

This version is fully backward compatible until and including version 0.18.0.


## Version 0.21.2 - 2022-02-14

* Fix importing projects

This version is fully backward compatible until and including version 0.18.0.


## Version 0.21.1 - 2022-01-28

* flowexec now returns different exit codes depending on the processing result

This version is fully backward compatible until and including version 0.18.0.


## Version 0.21.0 - 2022-01-26

* Fix wrong dependencies in Swagger plugin
* Implement basic schema inference for local CSV files
* Implement new `stack` mapping
* Improve error messages of local CSV parser

This version is fully backward compatible until and including version 0.18.0.


## Version 0.20.1 - 2022-01-06

* Implement detection of dependencies introduced by schema

This version is fully backward compatible until and including version 0.18.0.


## Version 0.20.0 - 2022-01-05

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

This version is fully backward compatible until and including version 0.18.0.


## Version 0.19.0 - 2021-12-13

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

This version is fully backward compatible until and including version 0.18.0.


## Version 0.18.0 - 2021-10-13

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


## Version 0.17.1 - 2021-06-18

* Bump CDH version to 6.3.4
* Fix scope of some dependencies
* Update Spark to 3.1.2
* Add new `values` relation


## Version 0.17.0 - 2021-06-02

* New Flowman Kernel and Flowman Studio application prototypes
* New ParallelExecutor
* Fix before/after dependencies in `count` target
* Default build is now Spark 3.1 + Hadoop 3.2   
* Remove build profiles for Spark 2.3 and CDH 5.15
* Add MS SQL Server plugin containing JDBC driver
* Speed up file listing for `file` relations  
* Use Spark JobGroups
* Better support running Flowman on Windows with appropriate batch scripts


## Version 0.16.0 - 2021-04-23

* Add logo to Flowman Shell
* Fix name of config option `flowman.execution.executor.class`
* Add new `groupedAggregate` mapping
* Reimplement target ordering, configurable via `flowman.execution.scheduler.class`
* Implement new assertions `columns` and `expression`


## Version 0.15.0 - 2021-03-23

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


## Version 0.14.2 - 2020-10-12

* Upgrade to Spark 2.4.7 and Spark 3.0.1
* Clean up dependencies
* Disable build of Docker image
* Update examples


## Version 0.14.1 - 2020-09-28

* Fix dropping of partitions which could cause issues on CDH6 


## Version 0.14.0 - 2020-09-10

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


## Version 0.13.1 - 2020-07-14

* Code improvements
* Do not implicitly set SPARK_MASTER in configuration
* Add support for CDH6
* Add support for Spark 3.0
* Improve support for Hadoop 3.x


## Version 0.13.0 - 2020-04-21

* Refactor Maven module structure
* Implement new Scala DSL for creating projects
* Fix ordering bug in target execution
* Merge `migrate` phase into `create` phase
* Rename `input` field to `mapping` in most targets
* Lots of minor code improvements


## Version 0.12.2 - 2020-04-06

* Fix type coercion of DecimalTypes


## Version 0.12.1 - 2020-01-15

* Improve support for Swagger Schema
* Fix infinite loop in recursiveSql


## Version 0.12.0 - 2020-01-09

* Add new RecursiveSqlMapping
* Refactor `describe` method of mappings
* Fix TemplateRelation to return correct partitions and fields
* Add `filter` attribute to many mappings


## Version 0.11.6 - 2019-12-17

* Improve build dependency management with DataSets


## Version 0.11.5 - 2019-12-16

* Update to newest Swagger V2 parser
* Workaround for bug in Swagger parser for enums
* Tidy up logging


## Version 0.11.4 - 2019-12-11

* Remove HDFS directories when dropping Hive table partitions


## Version 0.11.3 - 2019-12-06

* Improve migrations of HiveUnionTable
* Improve schema support in `copy` target 
 

## Version 0.11.2 - 2019-12-02

* Add new `earliest` mapping


## Version 0.11.1 - 2019-11-29

* Improve Hive compatibility of SQL generator for UNION statements


## Version 0.11.0 - 2019-11-28

* Add support for Spark 3.0 preview
* Remove HBase plugin
* Add optional `filter` to `readRelation` mapping
* Improve Hive compatibility of SQL generator for ORDER BY statements
* Fix target table search in Hive Union Table


## Version 0.10.2 - 2019-11-18

* Improve Impala catalog support


## Version 0.10.1 - 2019-11-14

* Add `error` output to `extractJson` mapping


## Version 0.10.0 - 2019-11-05

* Add new `hiveUnionTable` relation
* Add new `union` schema
* Support nested columns in deduplication
* Support nested Hive VIEWs
* Support Spark 2.4.4


## Version 0.9.1 - 2019-10-10

* Fix wrong Spark application name if configured via Spark config


## Version 0.9.0 - 2019-10-08

* Complete overhaul of job execution. No tasks anymore
* Improve Swagger schema support


## Version 0.8.3 - 2019-09-16

* Add configuration option for column insert position of `historize` mapping


## Version 0.8.2 - 2019-08-29

* Add optional filter condition to `latest` mapping


## Version 0.8.1 - 2019-08-29

* Improve generation of SQL code containing window functions


## Version 0.8.0 - 2019-08-28

* Add new metric system
* Add Hive view generation from mappings
* Support for Hadoop 3.1 and 3.2 (without Hive)
* Add `historize` mapping


## Version 0.7.1 - 2019-07-22

* Fix build with CDH-5.15 profile


## Version 0.7.0 - 2019-07-22

* Implement initial REST server
* Implement initial prototype of UI
* Implement new datasets for new tasks (copy/compare/...)


## Version 0.6.5 - 

* Add support for checkpoint directory


## Version 0.6.4 - 2019-06-20

* Implement column renaming in projections


## Version 0.6.3 - 2019-06-17

* CopyRelationTask also performs projection


## Version 0.6.2 - 2019-06-12

* `explode` mapping supports simple data types


## Version 0.6.1 - 2019-06-11

* Fix NPE in ShowRelationTask


## Version 0.6.0 - 2019-06-11

* Add multiple relations to `showRelation` task
* github-33: Add new `unit` mapping
* github-34: Fix NPE in Swagger schema reader
* github-36: Add new `explode` mapping

## Version 0.5.0 - 2019-05-28

* github-32: Improve handling of nullable structs
* github-30: Refactoring of whole specification handling
* github-30: Add new `template` mapping
* Add new `flatten` entry in assembler
* Implement new `flatten` mapping
* github-31: Fix handling of local project definition in flowexec


## Version 0.4.1 - 2019-05-11

* Add parameters to "job run" CLI
* Fix error handling of failing "build" and "clean" tasks


## Version 0.4.0 - 2019-05-08

* Add support for Spark 2.4.2
* Add new assemble mapping
* Add new conform mapping


## Version 0.3.0 - 2019-03-20

* Add null format for Spark
* Add deployment


## Version 0.1.2 - 2018-11-05

* Update to Spark 2.3.2
* Small fixes


## Version 0.1.1 - 2018-09-14

Small fixes


## Version 0.1.0 - 2018-07-20

Initial release
