# Execution Phases

As described in [Lifecycles and Phases](../concepts/lifecycle.md), Flowman uses so-called *execution phases* to
coordinate the different actions of each [build target](../spec/target/index.md) like creating a target table,
populating it with records etc.

Flowman offers some ways to influence this logic, which are described in detail on this page.


## Range Execution

Flowmans primary command line application [`flowexec`](../cli/flowexec/index.md) allows to execute a job for a range
of parameter values. Imagine for example, that your job contained a parameter called `processing_date`, which would
control what data would be transformed within each run in Flowman. This is typical for incremental transformation
pipelines, which only pick up new data, and leave other data unchanged.

### Example job
The following job will build two targets, `invoices_daily` and `transaction_daily`, which would only process data
which belongs to the given `processing_date`.
```yaml
jobs:
  daily:
    parameters:
      - name: processing_date
        type: date
        description: "Specifies the datetime in yyyy-MM-dd for which the result will be generated"
    targets:
      - invoices_daily
      - transactions_daily
  
targets:
  invoices_daily:
    kind: relation
    relation: invoices_daily
    mapping: invoices_daily
    partition:
      day: $processing_date

  transactions_daily:
    kind: relation
    relation: transactions_daily
    mapping: transactions_daily
    partition:
      day: $processing_date
```

You can now execute this job via `flowexec` as
```shell
flowexec job daily verify processing_date=2022-11-10
```
This will run the corresponding lifecycle with the phases `VALIDATE`, `CREATE`, `BUILD` and `VERIFY`.

But what happens in the case of an incident where some days have not been processed correctly? Of course, you could
simply restart `flowexec` with the corresponding parameters. But you can also instruct Flowman to run a whole date
range with a single invocation:
```shell
flowexec job daily verify processing_date:start=2022-11-01 processing_date:end=2022-11-10
```

## Skipping Phases
The command line above solves the problem of running the same job with multiple different parameters. But it will
probably do too much work, which is not required. For each set of parameters, all phases until `BUILD` will be
executed:
* `VALIDATE` - perform any sanity checks
* `CREATE` - create or migrate all target tables
* `BUILD` - populate all target tables with meaningful data
* `VERIFY` - verify the result
Maybe that is just too much of work being done, especially you probably don't want the `CREATE` phase to be executed 
over and over again for every date in the range of the parameter `processing_date` in the example above.

Luckily, Flowman allows you to specify when which phase is to be executed:

```yaml
jobs:
  daily:
    parameters:
      - name: processing_date
        type: date
        description: "Specifies the datetime in yyyy-MM-dd for which the result will be generated"

    targets:
      - invoices_daily
      - transactions_daily
      - customers_full
      - documentation

    executions:
      # Never execute VALIDATE phase
      - phase: validate
        cycle: never
      # The CREATE phase should only be executed once at the beginning
      - phase: create
        cycle: first
        # You can also omit the targets, if you want to have all of them
        targets: .*
      # You are allowed to specify a single phase more than once
      - phase: build
        cycle: always
        targets:
          # The following regular expressions matches all targets ending with "_daily"
          - .*_daily
      - phase: build
        cycle: last
        targets:
          # The following regular expressions matches all targets ending with "_full"
          - .*_full
      # Documentation should only be generated for the last entry in the execution sequence
      - phase: verify
        cycle: last
        targets: documentation
```
The `executions` section now described, when each phase should be executed for the given parameter range and which
targets should be executed. Each entry of the list has three attributes
* `phase` **(required)** *(type: string)* - the execution phase to be configured. You can have multiple entries
  per phase, these will be logically merged during execution.
* `cycle` **(optional)** *(type: string)* *(default: `always`)* - specifies when this block is active when
  cycling through a whole range of job parameters via command line. Possible values are:
  * `always` - this phase will be executed for all parameter instances
  * `never` - the corresponding phase will never be executed
  * `first` - only to be executed for the first parameter instance
  * `last` - only to be executed for the last parameter instance
* `targets` **(optional)** *(type: regex)* *(default: `.*`)* - list of regular expressions to match build targets which
  should be executed in the given phase. Note that you still need to specify all targets in the jobs `targets` main 
  list. This list in the `executions` sections acts as a filter on top of the jobs  main target list. Defaults to `.*`, 
  which simply selects all job targets for execution.

This means that the example above will eventually change the execution of the job as follows:
* `VALIDATE` (not listed) - will never be executed
* `CREATE` - will be executed only for the first date of `processing_date`, but again for all targets
* `BUILD` - will be executed for every date of `processing_date` for all targets ending with "_full". Moreover
during the last execution, all targets ending with "_daily" will be processed in addition.
* `VERIFY` - will be executed only for the last date of `processing_date` and only for the `documentation` target.

For example, if you start Flowman via
```shell
flowexec job daily verify processing_date:start=2022-11-01 processing_date:end=2022-11-05
```
then the whole execution plan will look as follows:

| processing_date | phase    | targets                                                           |
|-----------------|----------|-------------------------------------------------------------------|
| 2022-11-01      | CREATE   | invoices_daily, transactions_daily, customers_full, documentation |
| 2022-11-01      | BUILD    | invoices_daily, transactions_daily                                |
| 2022-11-02      | BUILD    | invoices_daily, transactions_daily                                |
| 2022-11-03      | BUILD    | invoices_daily, transactions_daily                                |
| 2022-11-04      | BUILD    | invoices_daily, transactions_daily, customers_full, documentation |
| 2022-11-04      | VERIFY   | documentation                                                     |      
