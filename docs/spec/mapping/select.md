# Select Mapping

A `select` mapping performs simple column-wise transformations, very similar to a simple SQL `SELECT ... FROM` 
statement. You specify a list of output columns together with an SQL expression for each of them.

## Example
```yaml
mappings:
  measurements:
    kind: select
    input: measurements-raw
    columns:
      usaf: "SUBSTR(raw_data,5,6)"
      wban: "SUBSTR(raw_data,11,5)"
      date: "SUBSTR(raw_data,16,8)"
      time: "SUBSTR(raw_data,24,4)"
      report_type: "SUBSTR(raw_data,42,5)"
      wind_direction: "SUBSTR(raw_data,61,3)"
      wind_direction_qual: "SUBSTR(raw_data,64,1)"
      wind_observation: "SUBSTR(raw_data,65,1)"
      wind_speed: "CAST(SUBSTR(raw_data,66,4) AS FLOAT)/10"
      wind_speed_qual: "SUBSTR(raw_data,70,1)"
      air_temperature: "CAST(SUBSTR(raw_data,88,5) AS FLOAT)/10"
      air_temperature_qual: "SUBSTR(raw_data,93,1)"
```

## Fields
* `kind` **(mandatory)** *(string)*: `select`

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

* `input` **(mandatory)** *(string)*:
The name of the input mapping
 
* `columns` **(mandatory)** *(map:string)*:
A map of column names to expressions. The result will contain exactly the columns
specified here by evaluating each corresponding SQL expression.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* select operation.


## Outputs
* `main` - the only output of the mapping
