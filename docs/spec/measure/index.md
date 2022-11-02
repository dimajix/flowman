# Measures

Flowman provides capabilities to assess data quality by taking *measures* from mappings and provide the result as
[metrics](../../cookbook/execution-metrics.md). This enables developer to build data quality dashboards using well known tools
like Prometheus and Grafana.


## Measure Syntax

```yaml
targets:
  my_measures:
    kind: measure
    measures:
      nulls:
        kind: sql
        query: "
          SELECT 
            SUM(col IS NULL) AS col_nulls
          FROM some_mapping
        "
```


## Common Fields

The common fields available in all mappings are as follows:

* `kind` **(mandatory)** *(string)*: This determines the type of the mapping. Please see the list of all available kinds
in the next section



## Measure Types

Flowman supports different kinds of measures, the following list gives you an exhaustive overview:

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
