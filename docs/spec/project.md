# Projects


## Project Specification

Flowman always requires a *Project* top level file containing general information (like a
projects name and version) and directories where to look for specifications. The project
file should be named `project.yml`, this way `flowexec` will directly pick it up when only
the directory is given on the command line.

A typical `project.yml` file looks as follows:

```
name: "example-project"
version: "1.0"
description: "My first example project"

modules:
 - config
 - model
 - mapping
 - target
 - job
```

## Fields

Each project supports the following fields:

* `name` **(mandatory)** *(string)*
The name of the overall project. This field will be used in a later Flowman version for 
sharing mappings and relations between different projects. 

* `version` **(optional)** *(string)*
The version currently is not used by Flowman, but can be used for the end-user to help keeping
track of which version of a project is currently being used.

* `description` **(optional)** *(string)*
A description of the overall project. Can be any text, is not used by Flowman otherwise

* `modules` **(mandatory)** *(list)*
The `modules` secion contains a list of *subdirectories* or *filenames* where Flowman should 
search for more YAML specification files. This helps to organize complex projects into 
different modules and/or aspects. The directory and file names are relative to the project 
file itself.


## Proposed Directory Layout

It is best practice to use a directory structure as follows:
```

```
