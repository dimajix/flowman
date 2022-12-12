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

But what happens in the case of an incident where some days have not been processed correctly? Of course you could
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
Maybe that is just too much, especially you probably don't want the `CREATE` phase to be executed over and over again.

Luckily, Flowman allows you to specify when which phase is to be execuetd:

```yaml
jobs:
  daily:
    parameters:
      - name: processing_date
        type: date
        description: "Specifies the datetime in yyyy-MM-dd for which the result will be generated"
    phases:
      validate: always
      create: first
      build: always
      verify: last
    targets:
      - invoices_daily
      - transactions_daily
```
The `phases` section now described, when each execution phase should be executed for the given range:
* `VALIDATE` - will be executed for every date of `processing_date`
* `CREATE` - will be executed only for the first date of `processing_date`
* `BUILD` - will be executed for every date of `processing_date`
* `VERIFY` - will be executed only for the last date of `processing_date`
