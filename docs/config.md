# Configuration Properties

Flowman supports some configuration properties, which influence the behaviour. These properties either can be set
on the command line via `--conf` (See [flowexec documentation](cli/flowexec.md)), or in the `config` section of the flow 
specification (see [module documentation](spec/module.md)) or in the naamespace configuration (see
[namespace documentation](spec/namespace.md))


## List of Configuration Properties
- `flowman.spark.enableHive` *(type: boolean)* *(default:true)*
If set to `false`, then Hive support will be disabled in Flowman.

- `floman.hive.analyzeTable` *(type: boolean)* *(default:true)*
If enabled (i.e. set to `true`), then Flowman will perform a `ANALYZE TABLE` for all Hive table updates.

- `flowman.home` *(type: string)*
Contains the home location of the Flowman installation. This will be set implicitly by the system environment 
variable `FLOWMAN_HOME`.

- `flowman.conf.directory` *(type: string)*
Contains the location of the Flowman configuration directory. This will be set implicitly by the system environment 
variable `FLOWMAN_CONF_DIR` or `FLOWMAN_HOME`.

- `flowman.plugin.directory` *(type: string)*
Contains the location of the Flowman plugin directory. This will be set implicitly by the system environment 
variable `FLOWMAN_PLUGIN_DIR` or `FLOWMAN_HOME`.

- `flowman.execution.target.forceDirty` *(type: boolean)* *(default:false)*
When enabled (i.e. set to `true`), then Flowman will treat all targets as being dirty. Otherwise Flowman will check
the existence of targets to decide if a rebuild is required.

- `flowman.default.target.outputMode` *(type: string)* *(default:OVERWRITE)*
Possible values are 
  - *`OVERWRITE`*: Will overwrite existing data. Only supported in batch output.
  - *`APPEND`*: Will append new records to existing data
  - *`UPDATE`*: Will update existing data. Only supported in streaming output.
  - *`IGNORE_IF_EXISTS`*: Silently skips the output if it already exists.
  - *`ERROR_IF_EXISTS`*: Throws an error if the output already exists
    
