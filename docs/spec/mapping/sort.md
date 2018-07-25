---
layout: page
title: Flowman Sort Mapping
permalink: /spec/mapping/sort.html
---
# Sort Mapping
The `sort` mapping simply sorts data by the specified columns and sort order. Note that 
downstream mappings may destroy the sort order again, so this should be the last operation
before you save a result using an output operation.

## Example
```
mappings:
  stations_sorted:
    kind: sort
    input: stations
    columns:
      - name: asc
      - date: desc
```

## Fields
* `kind` **(mandatory)** *(string)*: `select`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * NONE
  * DISK_ONLY
  * MEMORY_ONLY
  * MEMORY_ONLY_SER
  * MEMORY_AND_DISK
  * MEMORY_AND_DISK_SER

* `input` **(mandatory)** *(string)*:

* `columns` **(mandatory)** *(list:kv)*: 
Specifies the sort columns. The column names are specified as the key and the sort order as
the values. `asc` and `desc` are supported for sort order. 

## Description
