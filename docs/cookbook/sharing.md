# Sharing Entities between Projects

In bigger projects, it makes sense to organize data transformations like Flowman projects into separate subprojects,
so they can be maintained independently by possibly different teams. A classical example would be to have a different
Flowman project per source system (let it be your CRM system, your financial transaction processing system etc).
In a data lake environment, you probably want to implement independent Flowman projects to perform the first
technical transformations for each of these source systems. Then in the next layer, you want to create a more
complex and integrated data model built on top of these independent models.

In such scenarios, you want to share some common entity definitions between these projects, for example the Flowman
project for building the integrated data model may want to reuse the relations from the other projects. 

Flowman well supports these scenarios by the concept of imports.

## Example

First, you define a project which exports entities. Actually you might need to do nothing, since importing a project
will make all entities available to the importing side. But maybe your project also requires some variables to be
set, like the processing date. Typically, you would include such variables as job parameters: 
```yaml
# Project A, which contains shared resources 
jobs:
  # Define a base job with common environment variables  
  base:
    parameters:
      - name: processing_datetime
        type: timestamp
        description: "Specifies the datetime in yyyy-MM-ddTHH:mm:ss.Z for which the result will be generated"
      - name: processing_duration
        type: duration
        description: "Specifies the processing duration (either P1D or PT1H)"
    environment:
      - start_ts=$processing_datetime
      - end_ts=${Timestamp.add(${processing_datetime}, ${processing_duration})}
      - start_unixtime=${Timestamp.parse($start_ts).toEpochSeconds()}
      - end_unixtime=${Timestamp.parse($end_ts).toEpochSeconds()}

  # Define a specific job for daily processing
  daily:
    extends: base  
    parameters:
      - name: processing_datetime
        type: timestamp
    environment:
      - processing_duration=P1D 

  # Define a specific job for hourly processing
  hourly:
    extends: base
    parameters:
      - name: processing_datetime
    environment:
      - processing_duration=PT1H
```

The another project may want to access resources from project A, but within the context of the `export` job. This
can be achieved by declaring the dependency in an `imports` section within the project manifest:

```yaml
# project.yml of another project
name: raw-exporter

imports:
  # Import with no job and no (or default) parameters
  - project: project_b
    
  # Import project with specified job context
  - project: project_a
    # Optional specify the location
    location: ${project.basedir}/../project_a
    # The job may even be a variable, so different job context can be imported
    job: $period
    arguments:
      processing_datetime: $processing_datetime
```
Then you can easily access entities from `project_a` and `project_b` as follows:

```yaml
mappings:
  # You can access all entities from different projects by using the project name followed by a slash ("/")  
  sap_transactions:
    kind: filter
    input: project_b/transactions
    condition: "transaction_code = 8100"

relations:
  ad_impressions:
    kind: alias
    input: project_a/ad_impressions

jobs:
  main:
    parameters:
      - name: processing_datetime
        type: timestamp
    environment:
      # Set the variable $period, so it will be used to import the correct job
      - period=daily
```
