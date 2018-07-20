---
layout: page
title: Flowman Alias Mapping
permalink: /spec/mapping/alias.html
---
# Alias Mapping


## Example
```
mappings:
  facts_bidding:
    kind: alias
    input: facts_all
```

## Fields
* `kind` **(mandatory)** *(string)*: `alias`

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

## Description
An alias mapping simply provides an additional name to the input mapping.
