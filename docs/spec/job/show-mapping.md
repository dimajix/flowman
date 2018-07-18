---
layout: page
title: Flowman Show Mapping Task
permalink: /spec/job/show-mapping.html
---
# Show Mapping Task
The `show-mapping` task will print out some records of a specific mapping onto the console.
This is mainly for logging and debugging purpose.

## Example
```
jobs:
  main:
    tasks:
      - kind: show-mapping
        description: "Print some records of the mapping 'person'"
        input: person
        limit: 20
        columns:
          - first_name
          - last_name
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `sftp-upload`
* `description` **(optional)** *(type: string)* *(default: empty)*: 
A textual description of the task.
* `input` **(mandatory)** *(type: string)*: 
* `limit` **(optional)** *(type: integer)* *(default: 100)*:
* `columns` **(optional)** *(type: list:string)* *(default: empty)*:

## Description
