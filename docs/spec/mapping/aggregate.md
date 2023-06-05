# Aggregate Mapping

## Example
```yaml
mappings:
  cube_weather:
    kind: aggregate
    input: weather_details
    dimensions:
      - country
      - EXTRACT(YEAR FROM ts) AS year
      - EXTRACT(MONTH FROM ts) AS month
    aggregations:
      avg_temp: "AVG(temperature)"
      min_temp: "MIN(temperature)"
      max_temp: "MAX(temperature)"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `aggregate`

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
Specifies the list of dimensions to aggregate on. Since Flowman 0.18.1 you can also specify valid SQL expressions as dimensions.

* `aggregations` **(mandatory)** *(type: map:string)*:
Specifies the list of aggregations to perform. Each aggregation has a name (the key in the
map) and an aggregation expression. The name corresponds to the outgoing column name.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* aggregation, which corresponds to a `HAVING` filter in classical SQL.


## Outputs
* `main` - the only output of the aggregate mapping


## Description
Essentially the `aggregate` mapping performs a SQL `SELECT ... GROUP BY ...` operations. The
example above would be equivalent to the following SQL statement:
```
SELECT
    country,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(MONTH FROM ts) AS month,
    AVG(temperature) AS avg_temp,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp
FROM weather_details
GROUP BY country, EXTRACT(YEAR FROM ts), EXTRACT(MONTH FROM ts)    
```
