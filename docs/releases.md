# Releases

Flowman releases are done on an irregular basis, whenever new features are available or important issues have been
fixed.

## Official Releases

You will find all official Flowman releases on [GitHub](https://github.com/dimajix/flowman/releases).


## Past Releases 

The following gives an (incomplete) list of past releases of the last 12 months. This will help you to spot breaking 
changes over time.


### Version 0.30.0 - upcoming

* github-278: Parallelize execution of data quality checks
* github-282: Improve implementation for counting records
* github-288: Support reading local CSV files from fatjar
* github-290: Simplify specifying project name in fatjar
* github-291: Simplify create/destroy Relation interface
* github-292: Upgrade AWS EMR to 6.9
* github-289: Color log output via log4j configuration (requires log4j 2.x)
* Bump postgresql from 42.4.1 to 42.4.3 in /flowman-plugins/postgresql
* Bump loader-utils from 1.4.0 to 1.4.2
* github-293: [BUG] Fatal exceptions in parallel mapping instantiation cause deadlock
* github-273: Support projects contained in (fat) jar files
* github-294: [BUG] Parallel execution should not execute more targets after errors
* github-295: Create build profile for CDP 7.1 with Spark 3.2
* github-296: Update npm dependencies (vuetify & co)
* github-297: Parametrize when to execute a specific phase
* github-299: Move migrationPolicy and migrationStrategy from target into relation
* github-115: Implement additional build policy in relation target for forcing dirty
* github-298: Support fine-grained control when to execute each target of a job
* github-300: Implement new 'observe' mapping
* github-301: Upgrade Spark to 3.2.3
* github-302: Upgrade DeltaLake to 2.2.0
* github-303: Use multi-stage build for Docker image


### Version 0.29.0 - 2022-11-08

* github-260 Remove hive-storage-api from several plugins and lib
* github-261: Add descriptions to all pom.xml
* github-262: Verification of "relation" targets should only check existence
* github-263: Add filter condition to data quality checks in documentation
* github-265: Make JDBC dialects pluggable
* github-264: Provide "jars" for all plugins
* github-267: Add new flowman-spark-dependencies module to simplify dependency management
* github-269: Create 'iterativeSql' mapping
* github-270: Upgrade Spark to 3.3.1
* github-271: Upgrade Delta to 2.1.1
* github-272: Create build profile for AWS EMR 6.8.0
* github-273: Refactor file abstraction
* github-274: Print Flowman configuration to console


### Version 0.28.0 - 2022-10-07

* Improve support for MariaDB / MySQL as data sinks
* github-245: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-studio-ui
* github-246: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-server-ui
* github-247: Automatically generate YAML schemas as part of build process
* github-248: Bump scss-tokenizer and node-sass in /flowman-server-u
* github-249: Add new options -X and -XX to increase logging
* github-251: Support for log4j2 Configuration
* github-252: Move sftp target into separate plugin
* github-253: SQL Server relation should support explicit staging table
* github-254: Use DATETIME2 for timestamps in MS SQL Server
* github-256: Provide Maven archetype for simple Flowman projects
* github-258: Support clustered indexes in MS SQL Server


### Version 0.27.0 - 2022-09-09

* github-232: [BUG] Column descriptions should be propagates in UNIONs
* github-233: [BUG] Missing Hadoop dependencies for S3, Delta, etc
* github-235: Implement new `rest` hook with fine control
* github-229: A build target should not fail if Impala "COMPUTE STATS" fails
* github-236: 'copy' target should not apply output schema
* github-237: jdbcQuery relation should use fields "sql" and "file" instead of "query"
* github-239: Allow optional SQL statement for creating jdbcTable
* github-238: Implement new 'jdbcCommand' target
* github-240: [BUG] Data quality checks in documentation should not fail on NULL values
* github-241: Throw an error on duplicate entity definitions
* github-220: Upgrade Delta-Lake to 2.0 / 2.1
* github-242: Switch to Spark 3.3 as default
* github-243: Use alternative Spark MS SQL Connector for Spark 3.3
* github-244: Generate project HTML documentation with optional external CSS file


### Version 0.26.1 - 2022-08-03

* github-226: Upgrade to Spark 3.2.2
* github-227: [BUG] Flowman should not fail with field names containing "-", "/" etc
* github-228: Padding and truncation of CHAR(n)/VARCHAR(n) should be configurable


### Version 0.26.0 - 2022-07-27

* github-202: Add support for Spark 3.3
* github-203: [BUG] Resource dependencies for Hive should be case-insensitive
* github-204: [BUG] Detect indirect dependencies in a chain of Hive views
* github-207: [BUG] Build should not directly fail if inferring dirty status fails
* github-209: [BUG] HiveViews should not trigger cascaded refresh during CREATE phase even when nothing is changed
* github-211: Implement new `hiveQuery` relation
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

#### Breaking changes

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


### Version 0.25.1 - 2022-06-15

* github-195: [BUG] Metric "target_records" is not reset correctly after an execution phase is finished
* github-197: [BUG] Impala REFRESH METADATA should not fail when dropping views


### Version 0.25.0 - 2022-05-31

* github-184: Only read in *.yml / *.yaml files in module loader
* github-183: Support storing SQL in external file in `hiveView`
* github-185: Missing _SUCCESS file when writing to dynamic partitions
* github-186: Support output mode `OVERWRITE_DYNAMIC` for Delta relation
* github-149: Support creating views in JDBC with new `jdbcView` relation
* github-190: Replace logo in documentation
* github-188: Log detailed timing information when writing to JDBC relation
* github-191: Add user provided description to quality checks
* github-192: Provide example queries for JDBC metric sink


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
