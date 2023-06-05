# Job Commands
This command group operates on the level of individual jobs with different names than `main`.


## `list` - List all Jobs
The following command will list all jobs defined in a project
```shell
flowexec job list
```


## `validate|create|build|verify|truncate|destroy` - Execute Job Phase
This set of commands is used for *executing a job phase*, or a complete [lifecycle](../../concepts/lifecycle.md) 
containing multiple individual phases.
```shell
flowexec job <validate|create|build|verify|truncate|destroy> <job_name> <args>
```
This will execute the whole job by executing the desired [lifecycle](../../concepts/lifecycle.md) for the `main` job. 
The `<args>` parameter refers to the parameters as defined in a job. For example the following job defines one 
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
* `-f` or `--force` force execution of all targets in the job, even if Flowman considers the targets to be clean.
* `-t` or `--targets` explicitly specify targets to be executed. The targets can be specified as regular expressions
* `-d` or `--dirty` explicitly mark individual targets as being dirty, i.e. they need a rebuild. The targets can be
  specified as regular expressions. The difference between `-d` and `-t` is that while `-t` tells Flowman to only rebuild
  the specified targets if they are dirty, `-d` actually taints specific targets as being dirty, i.e. they need a rebuild.
  The difference between `-f` and `-d` is that `-f` marks *all* targets as being dirty, while you can explicitly select
  individual targets with `-d`.
* `-k` or `--keep-going` proceed with execution, in case of errors.
* `-j <n>` runs multiple job instances in parallel. This is very useful for running a job for a whole range of dates.
* `--dry-run` only simulate execution
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceding phases. For example
  the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
  `verify`. If this is not what you want, then use the option `-nl`


### Examples
In order to forcibly build  (i.e. run `VALIDATE`, `CREATE` and `BUILD` execution phases) the `main` job of a project 
stored in the subdirectory `examples/weather` which defines an (optional) parameter `year`, simply run

```shell
flowexec -f examples/weather job build main year=2018 --force
```

If you only want to execute the `BUILD` phase and skip the first two other phases, then you need to add the
command line option `-nl` or `--no-lifecycle` to skip all other phases of the lifecycle:

```shell
flowexec -f examples/weather job build main year=2018 -nl
```

### Executing Parameter Ranges
The following example will only execute the `BUILD` phase of the job `daily`, which defines a parameter
`processing_datetime` with type `datetime`. The job will be executed for the whole date range from 2021-06-01 until
2021-08-10 with a step size of one day. Flowman will execute up to four jobs in parallel (`-j 4`).

```shell
flowexec job build daily processing_datetime:start=2021-06-01T00:00 processing_datetime:end=2021-08-10T00:00 processing_datetime:step=P1D --target parquet_lineitem --no-lifecycle -j 4
```


## `job inspect` - Retrieving General Information
The `job inspect` commands provides some general information on an individual job, for example the list of all targets
within the job, parameters and environment variables. 

The following example inspects the job `main`:
```shell
flowexec -f examples/weather job inspect main
```
