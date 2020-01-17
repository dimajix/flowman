# Flowman Mapping Specification

Flowman uses the notion of `mappings` in order to specify the data flow and all data 
transformations. A mapping somewhat corresponds to a temporary view in the SQL world: You 
give a name to a mapping and specify its logic. Afterwards it is available for subsequent
operations like `SELECT`. Liek a temporary view, a mapping itself does not persist any data
and is only valid within a single run of flowman.

In contrast to a SQL view, there are different types of mappings, each performing different
operations. Most mappings require other mappings as their input. The most notably exception
is a `read` mapping, which reads data from a relation (and therefore doesn't have another
mapping as its input).

Mappings are instantiated lazily by flowman, i.e. the temporary view is created just when it
is needed for calculating the desired end result. For example when writing to a sink, Flowman
automatically determines and recursively resolves all required upstream mappings to provide the
final result to be written. You do not need to explicitly specify any execution order of 
mappings, Flowman takes care of this dependency management under the hood.
 

## Mapping Syntax

Mappings are specified in the `mappings` section of a file. For example
```
mappings:
  measurements-raw:
    kind: readRelation
    source: measurements-raw
    partitions:
      year:
        start: $start_year
        end: $end_year
    columns:
      raw_data: String

  measurements:
    kind: select
    input: measurements-raw
    columns:
      usaf: "SUBSTR(raw_data,5,6)"
      wban: "SUBSTR(raw_data,11,5)"
      date: "SUBSTR(raw_data,16,8)"
      time: "SUBSTR(raw_data,24,4)"
      wind_speed: "CAST(SUBSTR(raw_data,66,4) AS FLOAT)/10"
      wind_speed_qual: "SUBSTR(raw_data,70,1)"
      air_temperature: "CAST(SUBSTR(raw_data,88,5) AS FLOAT)/10"
      air_temperature_qual: "SUBSTR(raw_data,93,1)"
```
This specification defines two mappings: `measurements-raw` and `measurements`. Every mapping
needs to specify its `kind` which provides the desired type of operation. In this example
the first mapping `measurements-raw` of kind `readRelation` reads in input data from a 
relation called `measurements-raw`. The second mapping called `measurements` is of kind
`select` and will perform an operation comparable to an SQL `SELECT` to extract new columns
from its input mapping `measurements-raw` (which is just the first mapping).

There are some fields common to all mappings (like `kind`), but most other properties are 
specific to each kind.


## Common Fields

The common fields available in all mappings are as follows:

* `kind` **(mandatory)** *(string)*: This determines the type of the mapping. Please see the list of all available kinds
in the next section

* `broadcast` **(optional)** *(boolean)* *(default=false)*: This is a boolean flag providing 
a hint to the Spark execution optimizer. If set to `true`, Spark will broadcast the whole 
result of this mapping to all worker nodes for join operations. This may greatly speed up
execution if the result is small. Typical use case are small tables which map a small amount
of IDs to meaningful names.

* `cache` **(optional)** *(string)* *(default=NONE)*: This is another option to speed up 
computing. Caching will hold the result of the mapping in memory and/or on disk. This is
helpful when there are more than one output which rely on this input mapping. Allowed 
values are
  * `NONE` - no caching will take place
  * `MEMORY_ONLY` - cache all data in memory
  * `MEMORY_ONLY_SER` - cache all data in memory in a serialized representation
  * `MEMORY_AND_DISK` - try to cache all data in memory, fall back to disk
  * `DISK_ONLY` - cache all data on disk
 

## Mapping Types

Flowman supports different kinds of operations, the following list gives you an exhaustive
overview of all mappings implemented by Flowman

* [`aggregate`](aggregate.md): 
Use an [Aggregation](aggregate.md) mapping to perform aggregations for creating cubes

* [`alias`](alias.md): 
Use an [Alias](alias.md) to provide a new name to an existing mapping

* [`assemble`](assemble.md): 
Use an [Assemble](assemble.md) mapping to reassemble a nested data structure like from JSON or Avro

* [`coalesce`](coalesce.md):
Reduces the number of Spark partitions by logically merging partitions together.  

* [`conform`](conform.md): 
Using [Conforming](conform.md) you can apply a schema with a fixed order of fields and data types

* [`deduplicate`](deduplicate.md): 
Use a [Deduplicate](deduplicate.md) mapping for deduplicating records based on specific columns

* [`distinct`](distinct.md): 
Use a [Distinct](distinct.md) mapping for removing duplicates based on all columns

* [`explode`](explode.md): 
With [Explode](explode.md) you can create multiple records from an array

* [`extend`](extend.md): 
With [Extend](extend.md) you can add new columns to a mapping

* [`extractJson`](json-extract.md): 
[Extract columns from JSON](json-extract.md) for transforming raw JSON data into structured data 

* [`filter`](filter.md): 
Use [Filter](filter.md) to apply filter logic (essentially a `WHERE` condition)

* [`flatten`](flatten.md): 
Use [Flatten](flatten.md) to flatten the schema of a nested data structure into a simple list of columns.

* [`join`](join.md): 
Use a [Join](join.md) to merge two mappings based on a common key or a join condition

* [`latest`](latest.md):
Select the latest version of of multiple records with a unique key and a timestamp 

* [`project`](project.md): 
Using [Projections](project.md) you can select a subset of columns

* [`read` / `readRelation`](read-relation.md):
A [Read](read-relation.md) mapping reads in data from a relation.
 
* [`rebalance`](rebalance.md):
Shuffles data to produce a specified amount of Spark partitions with an approximately equal number
of records.

* [`repartition`](repartition.md):
Changes the number of (Spark) partitions of the result of a mapping.

* [`select`](select.md):
Evaluate arbitrary SQL expressions

* [`schema`](schema.md):
Apply a schema to input data

* [`sort`](sort.md):
[Sort](sort.md) records according to column values

* [`sql`](sql.md): An [SQL](sql.md) mapping allows to specify any SQL statement supported by
Spark SQL. You can reference any other mapping as tables within the statement.

* [`union`](union.md):
With a [Union](union.md) mapping, all records of multiple mappings can be appended to a single 
new mapping. Essentially works like a SQL `UNION ALL`. 

* [`unpackJson`](json-unpack.md): 
[Unpack JSON columns](json-unpack.md) for extracting individual columns containing raw JSON data.

* [`update`](update.md):
