# Prometheus Metric Sink

## Example
The following example configures a prometheus sink in a namespace. You would need to include this snippet
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
