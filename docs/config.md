# Configuration Properties

Flowman supports some configuration properties, which influence the behaviour. These properties either can be set
on the command line via `--conf` (See [flowexec documentation](cli/flowexec.md)), or in the `config` section of the flow 
specification (see [module documentation](spec/module.md)) or in the naamespace configuration (see
[namespace documentation](spec/namespace.md))


## List of Configuration Properties
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
  [Impala Catalog plugin](plugins/impala.md) whenever a Hive table is updated. The `REFRESH` statements will always
  be executed by the plugin.

- `flowman.execution.target.forceDirty` *(type: boolean)* *(default:false)*
When enabled (i.e. set to `true`), then Flowman will treat all targets as being dirty. Otherwise Flowman will check
the existence of targets to decide if a rebuild is required.
  
- `flowman.execution.executor.class` *(type: class)* *(default: `com.dimajix.flowman.execution.SimpleExecutor`)*
Configure the executor to use. The default `SimpleExecutor` will process all targets in the correct order
  sequentially. The alternative implementation `com.dimajix.flowman.execution.ParallelExecutor` will run multiple 
  targets in parallel (if they are not depending on each other)
  
- `flowman.execution.executor.parallelism` *(type: int)* *(default: 4)*
The number of targets to be executed in parallel, when the `ParallelExecutor` is used.

- `flowman.execution.scheduler.class` *(type: class)* *(default: `com.dimajix.flowman.execution.SimpleScheduler`)*
  Configure the scheduler to use. The default `SimpleScheduler` will sort all targets according to their dependency.

- `flowman.spark.eagerCache` *(type: boolean)* *(default: `false`)*
Turns on automatic eager caching of Spark jobs that reference a single cached DataFrame multiple times. This is to
avoid parallel computation of the same partitions, which can be seen in some scenarios.

- `flowman.default.relation.migrationPolicy` *(type: string)* *(default:`RELAXED`)*
Sets the default policy when to migrate tables. Possible values are:
  - *`STRICT`*: A migration will be initiated, whenever the physical table definition does not match the required
      one, even if the types would be compatible.
  - *`RELAXED`*: A migration will only be initiated, whenever the physical table definition is not sufficient for
    storing information with the required schema. If all types are compatible, not migration will be initiated.

- `flowman.default.relation.migrationStrategy` *(type: string)* *(default:`ALTER`)*
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

- `flowman.default.target.outputMode` *(type: string)* *(default:`OVERWRITE`)*
Sets the default target output mode. Possible values are 
  - *`OVERWRITE`*: Will overwrite existing data. Only supported in batch output.
  - *`APPEND`*: Will append new records to existing data
  - *`UPDATE`*: Will update existing data. Only supported in streaming output.
  - *`IGNORE_IF_EXISTS`*: Silently skips the output if it already exists.
  - *`ERROR_IF_EXISTS`*: Throws an error if the output already exists
Note that you can still explicitly specify a different output mode in each target.
    
- `flowman.default.target.rebalance` *(type: boolean)* *(default:false)*
If set to `true`, Flowman will try to write a similar records per each output file. Rebelancing might be an expensive
operation since it will invoke a Spark network shuffle. Note that you can still explicitly use different settings per
target. 

- `flowman.default.target.parallelism` *(type: int)* *(default:16)*
Sets the default number of output files per target. If set to zero or a negative value, the number of output files is 
implicitly determined by the number of internal Spark partitions, i.e. no explicit change will be performed. Note that 
you can still explicitly use different settings per target. 
