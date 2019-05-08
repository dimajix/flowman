---
layout: page
title: Flowman Aggregate Mapping
permalink: /spec/mapping/aggregate.html
---
# Aggregate Mapping

## Example
```
mappings:
  cube_weather:
    kind: aggregate
    input: weather_details
    dimensions:
      - country
      - year
      - month
    aggregations:
      avg_temp: "AVG(temperature)"
      min_temp: "MIN(temperature)"
      max_temp: "MAX(temperature)"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `alias`

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

* `dimensions` **(mandatory)** *(type: list:string)*:
Specifies the list of dimensions to aggregate on

* `aggregations` **(mandatory)** *(type: map:string)*:
Specifies the list of aggregations to perform. Each aggregation has a name (the key in the
map) and an aggregation expression. The name corresponds to the outgoing column name.

## Description
Essentially the `aggregate` mapping performs a SQL `SELECT ... GROUP BY ...` operations. The
example above would be equivalent to the following SQL statemtn:
```
SELECT
    country,
    year,
    month,
    AVG(temperature) AS avg_temp,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp
FROM weather_details
GROUP BY country, year, month    
```
