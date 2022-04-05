# Console Metric Sink

The `console` metric sink is the simplest possible way to publish [execution metrics](../../cookbook/metrics.md) and 
[data quality measures](../../cookbook/data-quality.md) as simple logging output on the console. Even if you
publish your metrics to a metric collector like [Promethus](prometheus.md), it is a good idea to also add the 
`console` metric sink, so you can see all metrics together with other log output


## Example
The following example configures a console sink in a namespace. You would need to include this snippet
for example in the `default-namespace.yml` in the Flowman configuration directory

```yaml
metrics:
  kind: console
```
