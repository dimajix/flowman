# Project Commands

By using project commands with `flowexec`, you can perform operations on the project level. Most of these operations
implicitly reference a `main` job, which must be defined within the project


## `verify|create|build|verify|truncate|destroy` - Lifecycle Commands
The most important command group is for executing a specific [lifecycle](../../concepts/lifecycle.md) or an individual 
phase for the whole project.
```shell
flowexec project <verify|create|build|verify|truncate|destroy> <args>
```
This will execute the whole job by executing the desired [lifecycle](../../concepts/lifecycle.md) for the `main` job. 
The `<args>` parameter refers to the parameters as defined in the `main` job. For example the following job defines one
parameter `processing_date` which needs to be specified on the command line.
```yaml
jobs:
  main:
    description: "Processes all outputs"
    parameters:
      - name: processing_date
        type: string
    targets:
      - some_hive_table
      - some_files
``` 
Additional parameters can be specified before or after `<args>` and are as follows:
* `-h` displays help
* `-f` or `--force` force execution of all targets in the project, even if Flowman considers the targets to be clean.
* `-t` or `--targets` explicitly specify targets to be executed. The targets can be specified as regular expressions.
* `-d` or `--dirty` explicitly mark individual targets as being dirty, i.e. they need a rebuild. The targets can be
  specified as regular expressions. The difference between `-d` and `-t` is that while `-t` tells Flowman to only rebuild
  the specified targets if they are dirty, `-d` actually taints specific targets as being dirty, i.e. they need a rebuild.
  The difference between `-f` and `-d` is that `-f` marks *all* targets as being dirty, while you can explicitly select
  individual targets with `-d`.
* `-k` or `--keep-going` proceed with execution, in case of errors.
* `-j <n>` or `--jobs <n>` execute multiple jobs in parallel
* `--dry-run` only simulate execution
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceding phases. For example
  the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
  `verify`. If this is not what you want, then use the option `-nl`

### Examples
In order to build a project (i.e. run `VALIDATE`, `CREATE` and `BUILD` execution phases) stored in the subdirectory
`examples/weather` which defines an (optional) parameter `year`, simply run

```shell
flowexec -f examples/weather project build year=2018
```

If you only want to execute the `BUILD` phase and skip the first two other phases, then you need to add the
command line option `-nl` or `--no-lifecycle` to skip the lifecycle:

```shell
flowexec -f examples/weather project build year=2018 -nl
```

### Executing Parameter Ranges
The following example will only execute the `BUILD` phase of the project, which defines a parameter
`processing_datetime` with type `datetime`. The job will be executed for the whole date range from 2021-06-01 until
2021-08-10 with a step size of one day. Flowman will execute up to four jobs in parallel (`-j 4`).

```shell
flowexec project build processing_datetime:start=2021-06-01T00:00 processing_datetime:end=2021-08-10T00:00 processing_datetime:step=P1D --target parquet_lineitem --no-lifecycle -j 4
```


## `inspect` - Retrieving General Information
The `project inspect` commands provides some general information, like a list of all jobs, targets, relations and
mappings and environment variables.

```shell
flowexec -f examples/weather project inspect
```
