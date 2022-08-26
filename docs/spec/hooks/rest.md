# REST Hook

The `rest` hook can be used to invoke an arbitrary web API at certain important lifecycle events during the project
execution of Flowman. This feature can be used to inform downstream system whenever a a job has been finished.


## Example
```yaml
job:
  main:
    hooks:
      - kind: rest
        url: http://$webhook_host/job&startdate=$URL.encode($start_ts)&enddate=$URL.encode($end_ts)&force=$force&status=$status
        when:
          category: job
          status: [SUCCESS, SUCCESS_WITH_ERRORS, FAILED]
      - kind: rest
        url: http://$webhook_host/lifecycle_success&job=$job
        when:
          category: lifecycle
          status: [SUCCESS, SUCCESS_WITH_ERRORS]
```
This example will call an external URL whenever a job has finished with `SUCCESS`, `SUCCESS_WITH_ERRORS` or `FAILED`
and it will also call a slightly different external URL whenever a lifecycle has finished with `SUCCESS` or 
`SUCCESS_WITH_ERRORS`.


## Fields
* `kind` **(mandatory)** *(type: string)*: `rest`

* `url` **(mandatory)** *(type: string)*:
  Http URL which should be called when a job is started. 

* `when` **(optional)** *(type: map[string,regex])*:
This map contains a list of conditions when the hook should actually be invoked. This allows you to only inform
external systems about specific events, for example when a job has been successfully finished. 


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
* `phase` - The execution phase. This is not available at the `lifecycle` level. Can be one of `VALIDATE`, `CREATE`, `BUILD`, `VERIFY`, `TRUNCATE` or `DESTROY`.
* `status` - The execution status. Can be one of `UNKNOWN`, `RUNNING`, `SUCCESS`, `SUCCESS_WITH_ERRORS`, `FAILED`, `ABORTED` or `SKIPPED`.
  Note that the execution status is only available at the end of the execution of a job or target. 
