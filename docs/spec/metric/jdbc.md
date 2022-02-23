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
or an directly embedded JDBC connection (like in the example).

* `commitTable` **(optional)** *(string)* *(default: flowman_metric_commits)*: The name of the table which will
get one entry ("commit") per publication of metrics.

* `commitLabelTable` **(optional)** *(string)* *(default: flowman_metric_commit_labels)*: The name of the table which will
  contain the labels of each commit.

* `metricTable` **(optional)** *(string)* *(default: flowman_metrics)*: The name of the table which will contain
the metrics.

* `metricLabelTable` **(optional)** *(string)* *(default: flowman_metric_labels)*: The name of the table which will
contain the labels of each metric.
