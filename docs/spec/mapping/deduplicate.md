---
layout: page
title: Flowman Deduplicate Mapping
permalink: /spec/mapping/deduplicate.html
---
# Deduplicate Mapping

## Example
```
mappings:
  persons_unique:
    kind: deduplicate
    input: persons
    columns:
      - first_name
      - last_name
      - birthdate
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `deduplicate`

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
Specifies the name of the input mapping

* `columns` **(optional)** *(type: string)* *(default: empty)*:
Specifies the names of the columns to use for deduplication. The result will still have the
full set of columns.

## Description
