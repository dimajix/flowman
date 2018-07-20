---
layout: page
title: Flowman Show Environment Task
permalink: /spec/job/show-environment.html
---
# Show Environment Task
The `show-environment` task is a small helper task, which simply prints either the whole
execution environment (i.e. all variables) or only a specified subset of variables with their
values onto the console. This is mainly for logging and debugging purpose.

## Example
```
jobs:
  main:
    tasks:
      - kind: show-environment
        description: "Print some basic project properties"
        variables:
          - project.name
          - project.version
          - project.basedir
          - project.filename
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `sftp-upload`
* `description` **(optional)** *(type: string)*: 
A textual description of the task.

* `variables` **(optional)** *(type: list:string)* *(default: empty)*:
Optionally you can specify a list of variables which should be printed onto the console. If 
you do not specify the list, then all variables will be printed.

## Description
