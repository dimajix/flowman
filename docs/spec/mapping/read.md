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
* `source` **(mandatory)** *(type: string)*:
* `partitions` **(optional)** *(type: map:partition)*:
* `columns` **(optional)** *(type: map:data_type)* *(default: empty):


## Description
