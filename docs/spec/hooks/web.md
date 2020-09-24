# Web Hook

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
endpoint. This is well supported by Flowman. In addition to the normal environment, the following variables can be
used:
* `job` - The name of the job
* `target` - The name of the target (can only be used in target specific URLs)
* `project` - The name of the project
* `version` - The version of the project
* `namespace` - The name of the namespace
* `phase` - The build phase (`create`, `build`, `verify`, `truncate` or `destroy`)
