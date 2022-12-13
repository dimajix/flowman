# Targets

From a top level perspective, Flowman works like a build tool like make or maven. Of course in contrast to classical
build tools, the project specification in Flowman also contains the logic to be build (normally that is separated
in source code files which get compiles or otherwise processed with additional tools).

Each target supports at least some [build phases](../../concepts/lifecycle.md)

## Common Fields

All Targets support the following common fields:

* `kind` **(mandatory)** *(type: string)*: The kind of the target

* `before` **(optional)** *(type: list:string)*: List of targets that can only be executed after this target

* `after` **(optional)** *(type: list:string)*: List of targets that need to be executed before this target

* `labels` **(optional)** *(type: map)*: Optional list of labels.


## Execution Model

Normally, targets are executed within the scope of a [job](../job/index.md). Each target listed in a job can be
active during some phases of the [lifecycle](../../concepts/lifecycle.md). For example when you execute the
`VERIFY` lifecycle, the following phases are actually executed by Flowman:
* `VALIDATE` - perform some optional pre-execution checks
* `CREATE` - create and/or migrate target data models
* `BUILD` - populate target data models with fresh data
* `VERIFY` - perform some optional post-execution checks

Depending on the *kind*, each target may contribute some work to each execution phase. Moreover, Flowman also checks
the state of each target. Only *dirty* targets will be rebuilt (except when you forcibly want to execute all targets
with the `--force` [command line switch](../../cli/flowexec/job.md)). 

A target is considered *dirty* for the current execution phase, when it needs some work to be performed. For example,
a JDBC table is considered dirty in the `CREATE` phase, when it does not exist yet or when its current schema 
(i.e. columns and data types) does not match the desired schema. Then Flowman will take care of creating and/or
[migrating](../../concepts/migrations.md) the JDBC table.

Typically, a target is considered dirty during the `BUILD` phase, when it does not contain any data (possibly only 
within the desired partition, see [relation target](relation.md) for more information).


## Target Types
Flowman supports different target types, each used for a different kind of physical entity or build recipe.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```

## Metrics

For each target Flowman provides the following execution metric:
* `metric`: "target_runtime"
* labels: 
  * `category`: "target"
  * `kind`: The kind of the target
  * `namespace`: The name of the namespace
  * `project`: The name of the project 
