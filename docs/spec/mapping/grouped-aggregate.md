# Grouped Aggregate Mapping

A `groupedAggregate` mapping is similar to *grouping sets* in SQL. It allows one to define common aggregates on a 
common fact table, but with different aggregation levels (i.e. aggregation groups).

## Example

```yaml
mappings:
  aggregations:
    kind: groupedAggregate
    input: facts_delivery
    aggregations:
      imps: "SUM(imps)"
      clicks: "SUM(clicks)"
      requests: "SUM(requests)"

    groups:
      adpod:
        filter: is_rtb IS TRUE
        dimensions:
          - device_setting
          - master_publisher
          - network
        aggregations:
          - imps
          - clicks
  
      delivery:
        having: imps > 0  
        dimensions:
          - advertiser
          - agency
        aggregations:
          - imps
          - requests 

targets:
  parquet_adpod:
    kind: relation
    relation: parquet_adpod
    mapping: aggregations:adpod

  parquet_delivery:
    kind: relation
    relation: parquet_delivery
    mapping: aggregations:delivery
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `groupedAggregate`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*:
  Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
  Cache mode for the results of this mapping. Supported values are
    * `NONE` - Disables caching of teh results of this mapping
    * `DISK_ONLY` - Caches the results on disk
    * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
    * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
    * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
    * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `input` **(mandatory)** *(type: string)*:
  Specifies the name of the input mapping to be aggregated

* `aggregations` **(mandatory)** *(type: map:string)*:
  Specifies the list of aggregations to perform. Each aggregation has a name (the key in the
  map) and an aggregation expression. The name corresponds to the outgoing column name.

* `groups` **(mandatory)** *(type: map:aggregation_group)*:
  Specifies a list of aggregation groups. Each aggregation group contains a `name`, a list of `dimensions` and a list
  of `aggregations`. The names of the `aggregations` must match the names of the top level `aggregations`

Each `aggregation_group` has the following fields:

Note that the `filter` is applied *before* the aggregation while *having* is applied after aggregation. This implies
that in `filter` you have access to all fields of the incoming mapping, while in `having` you only have access to
all aggregations and the dimensions defined in each group. If both options are viable, using `filter` is probably
more efficient, since it will reduce the records before aggregation.

* `dimensions` **(mandatory)** *(type: list:string)*:
  The list of dimensions within this aggregation group used for creating aggregation groups. This corresponds to
  the set of dimensions specified in a `GROUP BY` clause.
  
* `aggregations` **(optional)** *(type: list:string)*:
  The list of aggregations as defined in the `groupedAggregate`. When the aggregations list of a group is empty,
  then all aggregations will be used.

* `filter` **(optional)** *(type: string)*:
 A Spark SQL filter condition to be applied to all incoming records within this group *before* aggregation.
  
* `having`  **(optional)** *(type: string)*:
  A Spark SQL filter condition to be applied to all aggregates within this group after aggregation.


## Outputs
The outputs generated by the `groupedAggregate` mapping are given by the names of the aggregate groups. In addition
to these outputs, an additional internal output `cache` is also provided, which should not be used directly.
