---
layout: page
title: Flowman Read Mapping
permalink: /spec/mapping/read.html
---
# Read Mapping


## Example
```
mappings:
  measurements-raw:
    kind: read-relation
    source: measurements-raw
    partitions:
      year:
        start: $start_year
        end: $end_year
    columns:
      raw_data: String
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `read` or `read-relation`

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

* `source` **(mandatory)** *(type: string)*:
* `partitions` **(optional)** *(type: map:partition)*:
* `columns` **(optional)** *(type: map:data_type)* *(default: empty):


## Description
