# JDBC Hook

The `jdbc` hook can be used to execute an arbitrary SQL statement at certain important lifecycle events during the project
execution of Flowman. This feature can be used to inform downstream system whenever a job has been finished by
setting semaphore variables or calling stored procedures.


## Example
```yaml
job:
  main:
    hooks:
      - kind: jdbc
        # Embed a JDBC connection to use
        connection:
          kind: jdbc
          driver: "com.mysql.cj.jdbc.Driver"
          url: "jdbc:mysql://mysql-01/flowman"
          username: "flowman"
          password: "secret"
        # Specify SQL statements to execute
        jobStart: "INSERT INTO jobs VALUES('$project', '$job', '$phase', 'RUNNING')"
        jobSuccess: "INSERT INTO jobs VALUES('$project', '$job', '$phase', '$status')"
        targetStart: "INSERT INTO targets VALUES('$project', '$target', '$phase', 'RUNNING')"
        targetSuccess: "INSERT INTO targets VALUES('$project', '$target', '$phase', '$status')"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `jdbc`

* `connection` **(mandatory)** *(type: string or connection)*:
Either a reference (via name) or a directly embedded [JDBC connection](../connection/jdbc.md) to be used to talk with
the target database where the SQL commands are to be executed.

* `jobStart` **(optional)** *(type: string)*:
  SQL statement which should be executed when a job is started.

* `jobFinish` **(optional)** *(type: string)*:
  SQL statement which should be executed when a job has been finished, either successful or not.

* `jobSuccess` **(optional)** *(type: string)*:
  SQL statement which should be executed when a job has been successfully finished.

* `jobSkip` **(optional)** *(type: string)*:
  SQL statement which should be executed when a job is skipped.

* `jobFailure` **(optional)** *(type: string)*:
  SQL statement which should be executed when a job has failed.

* `targetStart` **(optional)** *(type: string)*:
  SQL statement which should be executed when a target is started.

* `targetFinish` **(optional)** *(type: string)*:
  SQL statement which should be executed when a target has been finished, either successful or not.

* `targetSuccess` **(optional)** *(type: string)*:
  SQL statement which should be executed when a target has been successfully finished.

* `targetSkip` **(optional)** *(type: string)*:
  SQL statement which should be executed when a target is skipped.

* `targetFailure` **(optional)** *(type: string)*:
  SQL statement which should be executed when a target has failed.


## Variables
In most scenarios, one wants to use environment variables in the SQL statements, for example to pass the job name or
execution status. This is well-supported by Flowman. In addition to the normal environment, the following variables can be
used:
* `project` - The name of the project
* `version` - The version of the project
* `namespace` - The name of the namespace
* `job` - The name of the job
* `target` - The name of the target (can only be used in target specific SQL statements)
* `category` - The category of the entity which is being processed. Can be `lifecycle`, `job` or `target`
* `kind` - The kind of the entity which is being processed.
* `name`- The name of the entity which is being processed.
* `phase` - The execution phase (`VALIDATE`, `CREATE`, `BUILD`, `VERIFY`, `TRUNCATE` or `DESTROY`)
* `status` - The execution status (`UNKNOWN`, `RUNNING`, `SUCCESS`, `SUCCESS_WITH_ERRORS`, `FAILED`, `ABORTED` or `SKIPPED`).
  Note that the execution status is only available at the end of the execution of a job or target. 
