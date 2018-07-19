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
* `input` **(mandatory)** *(string)*:

## Description
An alias mapping simply provides an additional name to the input mapping.
