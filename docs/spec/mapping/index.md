---
layout: page
title: Flowman Mapping Specification
permalink: /spec/mapping/index.html
---
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

* [`aggregate`](aggregate.html): 
Use an [Aggregation](aggregate.html) mapping to perform aggregations for creating cubes

* [`alias`](alias.html): 
Use an [Alias](alias.html) to provide a new name to an existing mapping

* [`coalesce`](coalesce.html):
Reduces the number of Spark partitions by logically merging partitions together.  

* [`deduplicate`](deduplicate.html): 
Use a [Deduplicate](deduplicate.html) mapping for deduplicating records based on specific columns

* [`distinct`](distinct.html): 
Use a [Distinct](distinct.html) mapping for removing duplicates based on all columns

* [`extend`](extend.html): 
With [Extend](extend.html) you can add new columns to a mapping

* [`filter`](filter.html): 
Use [Filter](filter.html) to apply filter logic (essentially a `WHERE` condition)

* [`join`](join.html): 
Use a [Join](join.html) to merge two mappings based on a common key or a join condition

* [`extractJson`](json-extract.html): 
[Extract columns from JSON](json-extract.html) for transforming raw JSON data into structured data 

* [`unpackJson`](json-unpack.html): 
[Unpack JSON columns](json-unpack.html) for extracting individual columns containing raw JSON data.

* [`conform`](conform.html): 
Using [Conforming](conform.html) you can apply a schema with a fixed order of fields and data types

* [`project`](project.html): 
Using [Projections](project.html) you can select a subset of columns

* [`read` / `readRelation`](read-relation.html):
A [Read](read-relation.html) mapping reads in data from a relation.
 
* [`rebalance`](rebalance.html):
Shuffles data to produce a specified amount of Spark partitions with an approximately equal number
of records.

* [`repartition`](repartition.html):
* [`select`](select.html):
* [`update`](update.html):
* [`latest`](latest.html):
* [`sort`](sort.html):
* [`sql`](sql.html): An [SQL](sql.html) mapping allows to specify any SQL statement supported by
Spark SQL. You can reference any other mapping as tables within the statement.

* [`union`](union.html):
With a [Union](union.html) mapping, all records of multiple mappings can be appended to a single 
new mapping. Essentially works like a SQL `UNION ALL`. 
