---
layout: page
title: Flowman Assemble Mapping
permalink: /spec/mapping/assemble.html
---
# Assemble Mapping

The assemble mapping is used to reassemble data with a possibly nested schema. For example it is possible to extract
some nested sub-entities, remove specific columns or subtrees or move subtrees to the top level.

## Examples
```
mappings:
  crm_updates:
    kind: assemble
    input: crm_events
    columns:
      - kind: rename
        columns:
          data_operation: dataOperation
      - kind: columns
        path: data
        drop:
          - faConfig.feeList
          - ebillingConfig
          - ebillingConfig.transmissionChannels
          - feeItems
          - negotiatedPrices
```

```
mappings:
  transmission_channel_explode:
    kind: assemble
    input: card_events_latest
    columns:
      - # Only keep some important fields from parent entity
        kind: rename
        columns:
          data_operation: dataOperation
          card_id: data.id
      - # Now extract and explode nested child entities
        kind: explode
        name: channel
        path: data.transmissionChannels

  transmission_channel_updates:
    kind: assemble
    input: transmission_channel_explode
    columns:
      - # Take everything except for channel
        kind: append
        drop: channel
      - # Now unpack all fields channel except maybe some
        kind: append
        path: channel
        drop: recipients
```

## Fields
* `kind` **(mandatory)** *(string)*: `assemble`

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

* `columns` **(mandatory)** *(objects)*:
This part contains the assembling specification. It consists of an array with the following possible sub elements:
  * `append` - Appends one or more columns from the input to the result
    * `kind` **(mandatory)** *(string)*: `append`
    * `path` **(optional)** *(string)*: 
        Specify a common subpath in the incoming data, from where columns should be extracted 
    * `keep` **(optional)** *(list: string)*: 
        Specifies a list of columns to be kept. The columns are relative to the optional path. Only the columns
        mentioned in the `keep` element will be appended, all other columns will be removed. 
    * `drop` **(optional)** *(list: string)*:
        Specified a list of columns to be removed. The columns are relative to the optional path. All columns not
        mentioned in the list of `drop` columns will be appended.
  * `explode` - Explodes a single array column into multiple output records
    * `kind` **(mandatory)** *(string)*: `explode`
    * `name` **(mandatory)** *(string)*:
        Specifies the name of the exploded column.
    * `path` **(optional)** *(string)*:
        Specifies the path to be exploded. The path has to be an array (or a child of an array)
  * `rename` - Renames some input columns
    * `kind` **(mandatory)** *(string)*: `rename`
    * `path` **(optional)** *(string)*:
        Specify a common subpath in the incoming data, from where columns should be extracted and renamed 
    * `columns` **(optional)** *(map: string)*:
        A map containing rename information. The key is the new column name and the value the incoming column name.
  * `lift` - Extracts specific columns of a nested path and appends these as simple columns
    * `kind` **(mandatory)** *(string)*: `lift`
    * `path` **(mandatory)** *(string)*:
        Specify the path in the incoming data which should be lifted to the top level
    * `columns` **(mandatory)** *(list: string)*:
        Specify all columns relative to the path of columns which should be lifted to the top level
  * `nest` - Collects some columns and nests these into a new sub structure
    * `kind` **(mandatory)** *(string)*: `nest`
    * `name` **(mandatory)** *(string)*:
        Specify the name of the new nested structure
    * `path` **(optional)** *(string)*:
        Specify the path in the incoming data which should be lifted to the top level
    * `keep` **(optional)** *(list: string)*:
        Specifies a list of columns to be kept. The columns are relative to the optional path. Only the columns
        mentioned in the `keep` element will be appended, all other columns will be removed. 
    * `drop` **(optional)** *(list: string)*:
        Specified a list of columns to be removed. The columns are relative to the optional path. All columns not
        mentioned in the list of `drop` columns will be appended.
  * `struct` - Collects some columns and nests these into a new sub structure
    * `kind` **(mandatory)** *(string)*: `struct`
    * `name` **(mandatory)** *(string)*:
        Specify the name of the new nested structure 
    * `columns` **(mandatory)** *(map: string)*:
        Specify all columns in the incoming data which should be put together as a new structure


## Description
