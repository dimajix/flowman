---
layout: page
title: Flowman Distinct Mapping
permalink: /spec/mapping/distinct.html
---
# Distinct Mapping


## Example
```
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `distinct`

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

* `input` **(mandatory)** *(type: string)*:


## Description
