# Flowman Configuration Properties

Flowman supports some configuration properties, which influence the behaviour. These properties either can be set
on the command line via `--conf` (See [flowexec documentation](cli/flowexec.md)), or in the `config` section of the flow 
specification (see [module documentation](spec/module.md)) or in the naamespace configuration (see
[namespace documentation](spec/namespace.md))


## List of Configuration Properties
- `flowman.spark.enableHive` *(type: boolean)* *(default:true)*
- `floman.hive.analyzeTable` *(type: boolean)* *(default:true)*
- `flowman.home` *(type: string)*
- `flowman.conf.directory` *(type: string)*
- `flowman.plugin.directory` *(type: string)*
- `flowman.execution.target.forceDirty` *(type: boolean)* *(default:false)*
- `flowman.default.target.outputMode` *(type: string)* *(default:OVERWRITE)*
Possible values are 
  - *`OVERWRITE`*: Will overwrite existing data. Only supported in batch output.
  - *`APPEND`*: Will append new records to existing data
  - *`UPDATE`*: Will update existing data. Only supported in streaming output.
  - *`IGNORE_IF_EXISTS`*: Silently skips the output if it already exists.
  - *`ERROR_IF_EXISTS`*: Throws an error if the output already exists
    
