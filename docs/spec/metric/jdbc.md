# JDBC Metric Sink

The `jdbc` metric sink is a very simple sink, which simply stores all collected metrics in a relational database.
Actually it is highly recommended setting up a proper monitoring using Prometheus or other supported and established
monitoring system instead of relying on a relational database.


## Example

```yaml
metrics:
  # Also add console metric sink (this is optional, but recommended)  
  - kind: console  
  # Now configure the Prometheus metric sink 
  - kind: jdbc
    # Specify labels on a commit level
    labels:
      project: ${project.name}
      version: ${project.version}
      phase: ${phase}
    connection:
      kind: jdbc
      driver: "com.mysql.cj.jdbc.Driver"
      url: "jdbc:mysql://mysql-01.ffm.dimajix.net/dimajix_flowman"
      username: "flowman-metrics"
      password: "my-secret-password"
```


## Fields

* `kind` **(mandatory)** *(string)*: `prometheus`

* `connection` **(mandatory)** *(string/connection)*: Either the name of a [`jdbc` connection](../connection/jdbc.md) 
or a directly embedded JDBC connection (like in the example).

* `tablePrefix` **(optional)** *(string)* *(default: flowman_)*: Prefix of all table names created by Flowman
get one entry ("commit") per publication of metrics.


## Tables

The following tables will be created by Flowman:

### Commit table `<table_prefix>_metric_commits`
This table contains one entry per metric publication. Each execution phase of a job will result in one metric
publication and therefore in one entry within this table.

| Column  | Data Type   | Constraints   | Description         |
|---------|-------------|---------------|---------------------|
| `id`    | `LONG`      | `PRIMARY KEY` | Serial ID of commit |
| `ts`    | `TIMESTAMP` |               | Timestamp of commit |


### Commit label table `<table_prefix>_metric_commit_labels`
The `<table_prefix>_metric_commit_labels` contains arbitrary labels attached to each commit.

| Column      | Data Type     | Constraints                                                | Description         |
|-------------|---------------|------------------------------------------------------------|---------------------|
| `commit_id` | `LONG`        | `FOREIGN KEY REFERENCES <table_prefix>_metric_commits(id)` | Serial ID of commit |
| `name`      | `VARCHAR(64)` |                                                            | Name of label       |
| `value`     | `VARCHAR(64)` |                                                            | Value of label      |


### Metric table `<table_prefix>_metrics`
The `<table_prefix>_metrics` contains the numerical metric values.

| Column      | Data Type     | Constraints                                                | Description                            |
|-------------|---------------|------------------------------------------------------------|----------------------------------------|
| `id`        | `LONG`        | `PRIMARY KEY`                                              | Serial ID of metric entry              |
| `commit_id` | `LONG`        | `FOREIGN KEY REFERENCES <table_prefix>_metric_commits(id)` | Serial ID of commit                    |
| `name`      | `VARCHAR(64)` |                                                            | Name of metric                         |
| `ts`        | `TIMESTAMP`   |                                                            | Timestamp when the metric was recorded |
| `value`     | `DOUBLE`      |                                                            | Value of label                         |


### Metric label table `<table_prefix>_metric_labels`
The `<table_prefix>_metric_labels` contains arbitrary labels attached to each metric.

| Column      | Data Type     | Constraints                                         | Description         |
|-------------|---------------|-----------------------------------------------------|---------------------|
| `metric_id` | `LONG`        | `FOREIGN KEY REFERENCES <table_prefix>_metrics(id)` | Serial ID of metric |
| `name`      | `VARCHAR(64)` |                                                     | Name of label       |
| `value`     | `VARCHAR(64)` |                                                     | Value of label      |


## Example Queries

When you set up the JDBC metric sink as above (and thereby store the phase as one label), you can use the following
query to retrieve all metrics of all targets:
```sql
SELECT
    m.name AS metric_name,
    m.value AS metric_value,
    fmc.ts AS commit_ts,
    fmcl.value AS phase,
    fml2.value AS target_name
FROM flowman_metrics m
LEFT JOIN flowman_metric_commits fmc on m.commit_id = fmc.id
INNER JOIN flowman_metric_commit_labels fmcl on fmc.id = fmcl.commit_id
    AND fmcl.name = 'phase'
INNER JOIN flowman_metric_labels fml1 on m.id = fml1.metric_id
    AND fml1.name = 'category' AND fml1.value = 'target'
INNER JOIN flowman_metric_labels fml2 on m.id = fml2.metric_id
    AND fml2.name = 'name'
```

Or you can get an overview of all collected metrics with the following query:
```sql
SELECT
    m.name AS metric_name,
    m.value AS metric_value,
    fmc.ts AS commit_ts,
    fmcl.value AS phase,
    fml1.value AS entity_category,
    fml2.value AS entity_name
FROM flowman_metrics m
LEFT JOIN flowman_metric_commits fmc on m.commit_id = fmc.id
INNER JOIN flowman_metric_commit_labels fmcl on fmc.id = fmcl.commit_id
    AND fmcl.name = 'phase'
INNER JOIN flowman_metric_labels fml1 on m.id = fml1.metric_id
    AND fml1.name = 'category'
INNER JOIN flowman_metric_labels fml2 on m.id = fml2.metric_id
    AND fml2.name = 'name'
```
