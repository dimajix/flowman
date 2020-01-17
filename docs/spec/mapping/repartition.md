
# Repartition Mapping

Shuffles data to produce a specified amount of Spark partitions by using the
specified columns to determine the partitioning.

## Example
```
mappings:
  repartitioned_customer_basket:
    kind: repartition
    input: customer_basket
    partitions: 200
    columns:
     - customer_id
     - product_id
   sort: true
```

## Fields
* `kind` **(mandatory)** *(string)*: `repartition`

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

* `columns` **(mandatory)** *(list:string)*:
The list of column names used for partitioning the data

* `partitions` **(optional)** *(integer)*:
The number of output partitions

* `sort` **(optional)** *(boolean)*:
Specifies if the records within each partition should also be sorted


## Description

This transformation can be used as part of a processing optimization where you want
to repartition the result of some mapping because you perform multiple join operations
on the same columns afterwards.
