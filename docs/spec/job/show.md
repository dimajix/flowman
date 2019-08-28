---
layout: page
title: Flowman Show Dataset Task
permalink: /spec/job/show.html
---
# Show Dataset Task
The Show task will print records of a dataset onto the console.

## Example
```
jobs:
  main:
    tasks:
      - kind: show
        description: "Show up to first 10 records of a mapping"
        input:
          kind: mapping
          mapping: my_mapping
        limit: 10
        columns:
          - id
          - value          
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `sftp-upload`
* `description` **(optional)** *(type: string)*: 
A textual description of the task.

* `input` **(mandatory)** *(type: dataset)*:

* `limit` **(mandatory)** *(type: integer)*:

* `columns` **(mandatory)** *(type: list<string>)*:

## Description
