# Namespace

On top of the very global settings, Flowman also supports so called *namespaces*. Each project is executed within the
context of one namespace, if nothing else is specified the *defautlt namespace*. Each namespace contains some 
configuration, such that different namespaces might represent different tenants or different staging environments.

## Example
```yaml
name: "default"

history:
  kind: jdbc
  connection: flowman_state
  retries: 3
  timeout: 1000

hooks:
  - kind: web
    jobSuccess: http://some-host.in.your.net/success&job=$URL.encode($job)&force=$force

connections:
  flowman_state:
    driver: $System.getenv('FLOWMAN_HISTORY_DRIVER', 'org.apache.derby.jdbc.EmbeddedDriver')
    url: $System.getenv('FLOWMAN_HISTORY_URL', $String.concat('jdbc:derby:', $System.getenv('FLOWMAN_HOME'), '/logdb;create=true'))
    username: $System.getenv('FLOWMAN_HISTORY_USER', '')
    password: $System.getenv('FLOWMAN_HISTORY_PASSWORD', '')

config:
  - spark.sql.warehouse.dir=$System.getenv('FLOWMAN_HOME')/hive/warehouse
  - hive.metastore.uris=
  - javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$System.getenv('FLOWMAN_HOME')/hive/db;create=true
  - datanucleus.rdbms.datastoreAdapterClassName=org.datanucleus.store.rdbms.adapter.DerbyAdapter

plugins:
  - flowman-delta
  - flowman-hbase
  - flowman-aws
  - flowman-azure
  - flowman-kafka
  - flowman-mariadb
  - flowman-swagger
  - flowman-openapi

metrics:
  kind: prometheus
  url: $System.getenv('URL_PROMETHEUS_PUSHGW', '')
  labels:
    job: flowman-aggregation
    instance: default
    namespace: ${namespace}

store:
  kind: file
  location: $System.getenv('FLOWMAN_HOME')/examples
```

## Fields

* `history` **(optional)** *(type: history)*:

* `store` **(optional)** *(type: store)*:

* `connections` **(optional)** *(type: list:connection)*:

* `metrics` **(optional)** *(type: list:metric-sink)*:
A list of metric sinks, where job metrics should be published to.

* `hooks` **(optional)** *(type: list:hook)*:
A list of hooks which will be called before and after each job and target is executed. Hooks provide some ways to
notify external systems (or possibly plugins) about the current execution status of jobs and targets.

* `plugins` **(optional)** *(type: list:string)*:
List of plugins to be loaded as part of the namespace
