# Jobs

In addition to a completely data centric data flow specification, Flowman also supports so called *jobs*, which simply
provide a list of targets to be built. The correct build order of all specified build targets is determined
automatically by Flowman by examining the artifacts being generated and required by each target. 


## Example
```yaml
jobs:
  main:
    description: "Processes all outputs"
    extends:
      - some_parent_job
    parameters:
      - name: processing_date
        type: string
        description: "Specifies the date in yyyy-MM-dd for which the job will be run"
    environment:
      - start_ts=$processing_date
      - end_ts=$Date.parse($processing_date).plusDays(1)
    hooks:
      - kind: web
        jobSuccess: http://0.0.0.0/success&startdate=$URL.encode($start_ts)&enddate=$URL.encode($end_ts)&period=$processing_duration&force=$force

    targets:
      - invoices_daily
      - transactions_daily
      - customers_full
      - documentation

    # The executions block allows fine-grained control over which targets should participate in which execution phase.
    # This helps to reduce the total amount of work, especially when executing parameter ranges.
    executions:
      # The following entry completely disables the VALIDATE phase
      - phase: validate
        cycle: never
      # The CREATE phase should only be executed once at the beginning of each parameter range
      - phase: create
        cycle: first
        # You can also omit the targets altogether, if you want to execute all of them. 
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

## Fields
* `description` **(optional)** *(type: string)*: 
A textual description of the job

* `extends` **(optional)** *(type: list:string)*:
A list of other job names, which should be extended by this job. All environment variables, parameters, build targets,
hooks and metrics will be inherited from the parent jobs. This helps to split up a big job into smaller ones or to
reuse some configuration in slightly different but related jobs.

* `targets` **(optional)** *(type: list:string)*:
A list of names of all targets that should be built as part of this job.
 
* `environment` **(optional)** *(type: list:string)*:
A list of `key=value` pairs for defining or overriding environment variables which can be accessed in expressions. 
You can also access the job parameters in the environment definition for deriving new values.
 
* `parameters` **(optional)** *(type: list:parameter)*:
A list of job parameters. Values for job parameters have to be specified for each job execution, be it either directly 
via the command line or via setting an environment variable in a derived job.
 
* `hooks` **(optional)** *(type: list:hook)*:
A list of [hooks](../hooks/index.md) which will be called before and after each job and target is executed. Hooks provide some ways to
notify external systems (or possibly plugins) about the current execution status of jobs and targets.
 
* `metrics` **(optional)** *(type: list:hook)*:
A list of metrics that should be published after job execution. See below for more details.

* `executions` **(optional)** *(type: list:execution)* (since Flowman 0.30.0):
This **optional** section provides fine-grained control over when the individual phases are to be executed. This allows 
to reduce the amount of redundant work when a whole (date) range is used for a parameter. Within this section you can 
explicitly state when each phase should be executed. Each entry of the list has three attributes
  * `phase` **(required)** *(type: string)* - the execution phase to be configured. You can have multiple entries
    per phase, these will be logically merged during execution.
  * `cycle` **(optional)** *(type: string)* *(default: `always`)* - specifies when this block is active when
    executing a whole range of job parameters via command line. Possible values are:
    * `always` - this phase will be executed for all parameter instances
    * `never` - the corresponding phase will never be executed
    * `first` - only to be executed for the first parameter instance
    * `last` - only to be executed for the last parameter instance
  * `targets` **(optional)** *(type: regex)* *(default: `.*`)* - list of regular expressions to match build targets.
    Note that you still need to specify all targets in the jobs `targets` main
    list. This list in the `executions` sections acts as a filter on top of the jobs  main target list. Defaults to `.*`,
    which simply selects all job targets for execution.

Please find more information about this option in the [Cookbook for Execution Phases](../../cookbook/execution-phases.md).
When this section is omitted, then all targets will participate in all execution phases, and all phases will be
executed for the full parameter range (if specified on the command line).


## Metrics

For each job Flowman provides the following execution metrics:
* `metric`: "job_runtime"
* labels: 
  * `category`: "job"
  * `kind`: "job"
  * `namespace`: The name of the namespace
  * `project`: The name of the project 


## Job Parameters

A Job optionally can have parameters, which play a special role. First they have to be specified when a job is run from 
the command line (via `flowexec job run param=value`).

Second flowman can be configured such that every run of a job is logged into a database. The log entry includes the 
job's name and also all values for all parameters. This way it is possible to identify individual runs of a job.

With these explanations in mind, you should only declare job parameters which have an influence on the data processing 
result (for example the processing date range). Other settings like credentials should not be provided as job 
parameters, but as normal environment variables instead.

Note that you can also execute a whole range of values for a given parameter as follows:
```shell
flowexec job build daily processing_datetime:start=2021-06-01T00:00 processing_datetime:end=2021-08-10T00:00 processing_datetime:step=P1D --target parquet_lineitem --no-lifecycle -j 4
```
This would assume that the job parameter `processing_datetime` is of type `date`.


## Job Isolation

Because a Job might be invoked with different values for the same set of parameters, each 
Job will be executed in a logically isolated environment, where all cached data is cleared
after the Job is finished. This way it is ensured that all mappings which rely on specific
parameter values, are reevaluated when the same Job is run mutliple times within a project.


## Publishing Metrics

Each job can define a set of metrics to be published. The job only contains the logical definition of metrics,
the type and endpoint of the receiver of the metrics is defined in the [namespace](../namespace.md).


## Sub Pages
```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
