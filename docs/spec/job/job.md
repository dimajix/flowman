---
layout: page
title: Flowman Job Specification
permalink: /spec/job/job.html
---
# Flowman Jobs

In addition to a completely data centric data flow specification, Flowman also supports so 
called *jobs*, which include specific *tasks* to be executed. A task simply is an action which
is executed, similar to a (simple) batch file.

Common tasks is the execution of one or more data flows via output tasks, data copy or 
upload tasks or simply printing some information onto the console. 

## Example
```
jobs:
  main:
    description: "Processes all outputs"
    parameters:
      - name: processing_date
        type: string
        description: "Specifies the date in yyyy-MM-dd for which the job will be run"
    environment:
      - start_ts=$processing_date
      - end_ts=$Date.parse($processing_date).plusDays(1)
    tasks:
      - kind: show-environment
        description: "Show all settings in the execution environment"

      - kind: output
        description: "Create all output files in HDFS"
        outputs:
          - export_table

      - kind: sftp-upload
        description: "Upload result to some sftp server"
        connection: my_sftp_server
        source: "$hdfs_export_basedir/export_table/${processing_date}"
        target: "${sftp_target}/export_table_${processing_date}.csv"
        merge: true
        overwrite: true
```

## Fields
* `description` **(optional)** *(type: string)*: 
A textual description of the job

* `environment` **(optional)** *(type: list:string)*:
A list of `key=value` pairs for defining or overriding environment variables which can be
accessed in expressions. You can also access the job parameters in the environment definition
for deriving new values.
 
* `parameters` **(optional)** *(type: list:parameter)*:
A list of job parameters. Values for job parameters have to be specified for each job
execution, be it either directly via the command line or via a `call` task as part of a
different job in the same project.
 
* `logged` **(optional)** *(type: boolean)* *(default: true)*:
A boolean indicating whether the run should be logged in the Flowman job run database. Note
that such a database needs to be configured in the namespace definition.
 
* `tasks` **(required)** *(type: list:task)*:
A list of tasks that should be executed. If one task fails, all following tasks will not be
executed any more.

* `cleanup` **(optional)** *(type: list:task)* *(default: empty)*:
A list of tasks, which should also be executed in the case of an error. This block will always
be executed after the `tasks` list.

* `failure` **(optional)** *(type: list:task)* *(default: empty)*:
A list of tasks, which will be executed when a task fails.

## Metrics

For each job Flowman provides the following execution metrics:
* metric: "job_runtime"
* labels: 
  * category: "job"
  * kind: "job"
  * namespace: 
  * project: 



## Job Parameters

A Job optionally can have parameters, which play a special role. First they have to be
specified when a job is run from the command line (via `flowexec job run param=value`) or
when a job is called is a subroutine via a [`call`](call.html) task.

Second flowman can be conifgured such that every run of a job is logged into a database. The
log entry includes the job's name and also all values for all parameters. This way it is 
possible to identify individual runs of a job.

With these explanations in mind, you should only declare job parameters which have a influence
on the data processing result (for example the processing date range). Other settings like
credentials should not be provided as job parameters, but as normal environment variables
instead.

## Job Isolation

Because a Job might be invoked with different values for the same set of parameters, each 
Job will be executed in a logically isolated environment, where all cached data is cleared
after the Job is finished. This way it is ensured that all mappings which rely on specific
parameter values, are reevaluated when the same Job is run mutliple times within a project.

## Metrics

Each job can define a set of metrics to be published
