# Web Hook

The `web` hook can be used to invoke an arbitrary web API at certain important lifecycle events during the project
execution of Flowman. This feature can be used to inform downstream system whenever a a job has been finished.
You may also want to have a look at the [`rest` hook](rest.md), which offers a similar functionality but with different
configuration options.

## Example
```yaml
job:
  main:
    hooks:
      - kind: web
        jobSuccess: http://$webhook_host/success&startdate=$URL.encode($start_ts)&enddate=$URL.encode($end_ts)&period=$processing_duration&force=$force
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `web`

* `jobStart` **(optional)** *(type: string)*:
 Http URL which should be called when a job is started. 

* `jobFinish` **(optional)** *(type: string)*: 
 Http URL which should be called when a job has been finished, either successful or not. 

* `jobSuccess` **(optional)** *(type: string)*: 
 Http URL which should be called when a job has been successfully finished.

* `jobSkip` **(optional)** *(type: string)*: 
 Http URL which should be called when a job is skipped.

* `jobFailure` **(optional)** *(type: string)*: 
 Http URL which should be called when a job has failed.

* `targetStart` **(optional)** *(type: string)*:
 Http URL which should be called when a target is started. 

* `targetFinish` **(optional)** *(type: string)*: 
 Http URL which should be called when a target has been finished, either successful or not. 

* `targetSuccess` **(optional)** *(type: string)*: 
 Http URL which should be called when a target has been successfully finished.

* `targetSkip` **(optional)** *(type: string)*: 
 Http URL which should be called when a target is skipped.

* `targetFailure` **(optional)** *(type: string)*: 
 Http URL which should be called when a target has failed.


## Variables
In most scenarios, one wants to use environment variables in the URLs, for example to pass the job name to a REST
endpoint. This is well-supported by Flowman. In addition to the normal environment, the following variables can be
used:
* `project` - The name of the project
* `version` - The version of the project
* `namespace` - The name of the namespace
* `job` - The name of the job
* `target` - The name of the target (can only be used in target specific URLs)
* `category` - The category of the entity which is being processed. Can be `lifecycle`, `job` or `target`
* `kind` - The kind of the entity which is being processed.
* `name`- The name of the entity which is being processed.
* `phase` - The execution phase (`VALIDATE`, `CREATE`, `BUILD`, `VERIFY`, `TRUNCATE` or `DESTROY`)
* `status` - The execution status (`UNKNOWN`, `RUNNING`, `SUCCESS`, `SUCCESS_WITH_ERRORS`, `FAILED`, `ABORTED` or `SKIPPED`).
  Note that the execution status is only available at the end of the execution of a job or target. 
