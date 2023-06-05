# Prometheus Metric Sink

The `prometheus` metric sink allows you to publish collected metrics to a Prometheus push gateway. This then can
be scraped by a Prometheus server.


## Example
The following example configures a `prometheus` sink in a namespace. You would need to include this snippet
for example in the `default-namespace.yml` in the Flowman configuration directory

```yaml
metrics:
  # Also add console metric sink (this is optional, but recommended)  
  - kind: console  
  # Now configure the Prometheus metric sink 
  - kind: prometheus
    url: $System.getenv('URL_PROMETHEUS_PUSHGW', '')
    labels:
      job: flowman-aggregation
      instance: default
      namespace: ${namespace}
```

## Fields

* `kind` **(mandatory)** *(string)*: `prometheus`

* `url` **(mandatory)** *(string)*:  Specifies the URL of the Prometheus push gateway

* `labels` **(optional)** *(map)*: Specifies an additional set of labels to be pushed to Prometheus. This set
of labels will determine the path in Prometheus push gateway, under which all metrics will be atomically published.
