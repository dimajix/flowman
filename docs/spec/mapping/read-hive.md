# Read Hive Mapping

The `readHibve` mapping is a shortcut for reading from a Hive table or Hive view without explicitly creating a 
[Hive relation](../relation/hiveTable.md) in Flowman.

## Example
```yaml
mappings:
  measurements-raw:
    kind: readHive
    database: weather
    table: measurements
    columns:
      country: String
      air_temperature: Double
    filter: "country IS NOT NULL"
```
This would be equivalent to the following more verbose specification:
```yaml
relations:
    measurements-raw:
    kind: hiveTable
    database: weather
    table: measurements

mappings:
  measurements-raw:
    kind: readRelation
    relation: measurements-raw
    columns:
      country: String
      air_temperature: Double
    filter: "country IS NOT NULL"
```
The main scenario for using the shortcut is when reading from existing Hive tables, which are not managed by the
current Flowman project. Whenever you write to a Hive table within Flowman, you need to create the `hiveTable` relation
anyway, so you should also use `readRelation` in favor of `readHive`. 


## Fields

* `kind` **(mandatory)** *(type: string)*: `read` or `readRelation`

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

* `database` **(mandatory)** *(string)*:
  Defines the Hive database where the table is defined. When no database is specified, the
  table is accessed without any specific qualification, meaning that the default database
  will be used.

* `table` **(mandatory)** *(string)*:
  Contains the name of the Hive table or Hive view.
* 
* `columns` **(optional)** *(type: map:data_type)* *(default: empty):
Specifies the list of columns and types to read from the relation. This schema will be applied to the records after 
  they have been read and interpreted by the underlying source. This schema will also be used as a substitute for schema
  inference and therefore can be very helpful when using [`mock`](mock.md) mappings.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied for reading only a subset of records. The filter is applied
  *after* the schema as specified in `columns` is applied. This means that if you are using `columns`, then you
  can only access these columns in the `filter` expression.


## Outputs
* `main` - the only output of the mapping
