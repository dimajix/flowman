# Configuration Properties

Flowman supports some configuration properties, which influence the behaviour. These properties either can be set
on the command line via `--conf` (See [flowexec documentation](../cli/flowexec.md)), or in the `config` section of the flow 
specification (see [module documentation](../spec/module.md)) or in the namespace configuration (see
[namespace documentation](../spec/namespace.md))


## List of Configuration Properties

### General Properties

- `flowman.spark.enableHive` *(type: boolean)* *(default:true)*
If set to `false`, then Hive support will be disabled in Flowman.

- `flowman.home` *(type: string)*
Contains the home location of the Flowman installation. This will be set implicitly by the system environment 
variable `FLOWMAN_HOME`.

- `flowman.conf.directory` *(type: string)*
Contains the location of the Flowman configuration directory. This will be set implicitly by the system environment 
variable `FLOWMAN_CONF_DIR` or `FLOWMAN_HOME`.

- `flowman.plugin.directory` *(type: string)*
Contains the location of the Flowman plugin directory. This will be set implicitly by the system environment 
variable `FLOWMAN_PLUGIN_DIR` or `FLOWMAN_HOME`.

- `flowman.hive.analyzeTable` *(type: boolean)* *(default:true)*
  If enabled (i.e. set to `true`), then Flowman will perform a `ANALYZE TABLE` for all Hive table updates.

- `flowman.impala.computeStats` *(type: boolean)* *(default:true)*
  If enabled (i.e. set to `true`), then Flowman will perform a `COMPUTE STATS` within the 
  [Impala Catalog plugin](../plugins/impala.md) whenever a Hive table is updated. The `REFRESH` statements will always
  be executed by the plugin.


### Execution related Properties

- `flowman.execution.target.forceDirty` *(type: boolean)* *(default:false)* (since Flowman 0.14.0)
When enabled (i.e. set to `true`), then Flowman will treat all targets as being dirty. Otherwise, Flowman will check
the existence of targets and/or the history to decide if a rebuild is required.

- `flowman.execution.target.useStateStore`  *(type: boolean)* *(default:false)* (since Flowman 0.20.0)
  When enabled (i.e. set to `false`), then Flowman will ignore the history information to check if a target needs a
  rebuild. Otherwise, the history store will be trusted for deciding if a target needs a rebuild.
  
- `flowman.execution.executor.class` *(type: class)* *(default: `com.dimajix.flowman.execution.SimpleExecutor`)* (since Flowman 0.16.0)
Configure the executor to use. The default `SimpleExecutor` will process all targets in the correct order
  sequentially. The alternative implementation `com.dimajix.flowman.execution.ParallelExecutor` will run multiple 
  targets in parallel (if they are not depending on each other)

- `flowman.execution.executor.parallelism` *(type: int)* *(default: 4)* (since Flowman 0.16.0)
  The number of targets to be executed in parallel, when the `ParallelExecutor` is used.

- `flowman.execution.mapping.parallelism` *(type: int)* *(default: 1)* (since Flowman 0.19.0)
The number of mappings to be processed in parallel. Increasing this number may help in scenarios where many 
relations are read from and their initial setup is slow (for example due to slow directory listings). With the
default value of 1, the parallelism is completely disabled and a non-threaded code path is used instead.

- `flowman.execution.mapping.schemaCache` *(type: boolean)* *(default: true)* (since Flowman 0.22.0)
Turn on/off caching of schema information of mappings. Caching this information (which is enabled per default) can
 speed up schema inference, which is used for `mapping` schemas and when creating the documentation of mappings. Turning
 off the cache is mainly for debugging purposes.

- `flowman.execution.relation.schemaCache` *(type: boolean)* *(default: true)* (since Flowman 0.22.0)
Turn on/off caching of schema information of relations. Caching this information (which is enabled per default) can
 speed up schema inference, which is used for `relation` schemas and when creating the documentation of relations and.
 mappings. Turning off the cache is mainly for debugging purposes.

- `flowman.execution.scheduler.class` *(type: class)* *(default: `com.dimajix.flowman.execution.DependencyScheduler`)* (since Flowman 0.16.0)
  Configure the scheduler to use, which essentially decides which target to build next.
  - The default `DependencyScheduler` will sort all targets according to their dependency.
  - The simpler `ManualScheduler` will simply respect the order of targets as specified in a job. This may not work
    well with parallel execution if targets have dependencies.

- `flowman.spark.eagerCache` *(type: boolean)* *(default: `false`)*
Turns on automatic eager caching of Spark jobs that reference a single cached DataFrame multiple times. This is to
avoid parallel computation of the same partitions, which can be seen in some scenarios.


### Relation related Properties

- `flowman.default.relation.migrationPolicy` *(type: string)* *(default:`RELAXED`)* (since Flowman 0.18.0)
Sets the default policy when to migrate tables. Possible values are:
  - *`STRICT`*: A migration will be initiated, whenever the physical table definition does not match the required
      one, even if the types would be compatible.
  - *`RELAXED`*: A migration will only be initiated, whenever the physical table definition is not sufficient for
    storing information with the required schema. If all types are compatible, not migration will be initiated.

- `flowman.default.relation.migrationStrategy` *(type: string)* *(default:`ALTER`)* (since Flowman 0.18.0)
Sets the strategy to use how tables should be migrated. Possible values are:
  - *`NEVER`* even if a migration would be required, it will not be performed. No error will be generated.
  - *`FAIL`* even if a migration would be required, it will not be performed, instead an error will be generated.
  - *`ALTER`* Flowman will try to modify an existing table with `ALTER TABLE` statements. This will preserve all
    current contents of the record. On the other hand, this strategy might not be well supported with all table types 
    and/or changes.
  - *`ALTER_REPLACE`* Flowman will try to modify an existing table with `ALTER TABLE` statements. If that is not
    possible, the table will be dropped and recreated. Note that all contents will be lost if a table replacement
    is required.
  - *`REPLACE`* If a migration is required, Flowman will always replace the existing table with a new one.
    Note that all contents will be lost.

- `flowman.default.relation.input.columnMismatchPolicy` *(type: string)* *(default:`IGNORE`)* (since Flowman 0.20.0)
  Defines how Flowman should handle a mismatch between the actual columns of a relation when reading from it and the
  columns as defined in the relation. Per default, Flowman ignores any mismatch and simply passes through the schema
  of the actual relation. See [relations](../spec/relation/index.md) for possible options and more details.
- `flowman.default.relation.input.typeMismatchPolicy` *(type: string)* *(default:`IGNORE`)* (since Flowman 0.20.0)
  Defines how Flowman should handle a mismatch between the types of the actual schema of a relation when reading from 
  it and the types of the schema as defined in the relation. Per default, Flowman ignores any mismatch and simply passes 
  through the types of the actual relation. See [relations](../spec/relation/index.md) for possible options and more details.
- `flowman.default.relation.output.columnMismatchPolicy` *(type: string)* *(default:`ADD_REMOVE_COLUMNS`)* (since Flowman 0.20.0)
  Defines how Flowman should handle a mismatch of columns of records being written to a relation and the relations
  actual defined columns. Per default Flowman will add/remove columns to/from records such that they match the current
  physical layout. See [relations](../spec/relation/index.md) for possible options and more details.
- `flowman.default.relation.output.typeMismatchPolicy` *(type: string)* *(default:`CAST_ALWAYS`)* (since Flowman 0.20.0)
  Defines how Flowman should handle a mismatch of columns of records being written to a relation and the relations
  actual defined columns. Per default Flowman will add/remove columns to/from records such that they match the current
  physical layout. See [relations](../spec/relation/index.md) for possible options and more details.


### Target related Properties

- `flowman.default.target.verifyPolicy` *(type: string)* *(default:`EMPTY_AS_FAILURE`)* (since Flowman 0.22.0)
Defines the default target policy that is used during the `VERIFY` execution phase. The setting controls how Flowman
interprets an empty table. Normally you'd expect that all target tables contain records, but this might not always
be the case, for example when the source tables are already empty. Possible values are
  - *`EMPTY_AS_FAILURE`*: Flowman will report an empty target table as an error in the `VERIFY` phase.
  - *`EMPTY_AS_SUCCESS`*: Flowman will ignore empty tables, but still check for existence in the `VERIFY` phase.
  - *`EMPTY_AS_SUCCESS_WITH_ERRORS`*: An empty output table is handled as partially successful.

- `flowman.default.target.outputMode` *(type: string)* *(default:`OVERWRITE`)*
Sets the default target output mode. Possible values are 
  - *`OVERWRITE`*: Will overwrite existing data. Only supported in batch output.
  - *`APPEND`*: Will append new records to existing data
  - *`UPDATE`*: Will update existing data. Only supported in streaming output.
  - *`IGNORE_IF_EXISTS`*: Silently skips the output if it already exists.
  - *`ERROR_IF_EXISTS`*: Throws an error if the output already exists
Note that you can still explicitly specify a different output mode in each target.
    
- `flowman.default.target.rebalance` *(type: boolean)* *(default:false)* (since Flowman 0.15.0)
If set to `true`, Flowman will try to write a similar records per each output file. Rebelancing might be an expensive
operation since it will invoke a Spark network shuffle. Note that you can still explicitly use different settings per
target. 

- `flowman.default.target.parallelism` *(type: int)* *(default:16)* (since Flowman 0.15.0)
Sets the default number of output files per target. If set to zero or a negative value, the number of output files is 
implicitly determined by the number of internal Spark partitions, i.e. no explicit change will be performed. Note that 
you can still explicitly use different settings per target. 


### Workarounds

Sometimes some workarounds are required, especially for non-quite-open-source Big Data platforms.

- `flowman.workaround.analyze_partition` *(type: boolean)* (since Flowman 0.18.0)
Enables a workaround for CDP 7.1, where ANALYZE TABLES wouldn't always work correctly (especially in unittests). The
  workaround is enabled per default if the Spark version matches ?.?.?.7.?.?.?.+ (i.e. 2.4.0.7.1.6.0-297) AND if 
  the Spark repository url contains "cloudera".
  

## Example

You can set the properties either at namespace level or at project level in the `config` section as follows:
```yaml
# default-namespace.yml

config:
  # Generic Spark configs  
  - spark.sql.suffle.partitions=20
  - spark.sql.session.timeZone=UTC
  # Flowman specific config  
  - flowman.workaround.analyze_partition=true
  - flowman.default.relation.migrationStrategy=FAIL
```
