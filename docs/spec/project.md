# Projects

The specification of all relations, data transformations and build targets is done within Flowman projects. Each
project has a top level project descriptor which mainly contains some meta information like project name and
version and a list of subdirectories, which contain the entity definitions.


## Project Specification

Flowman always requires a *Project* top level file containing general information (like a projects name and version) 
and directories where to look for specifications. The project file should be named `project.yml`, this way `flowexec` 
and `flowshell` will directly pick it up when only the directory is given on the command line.

A typical `project.yml` file looks as follows:

```yaml
name: "example-project"
version: "1.0"
description: "My first example project"

modules:
 - config
 - model
 - mapping
 - target
 - job
  
imports:
  - project: other_project 

  - project: commons
    arguments:
      processing_date: $processing_date
```

## Fields

Each project supports the following fields:

* `name` **(mandatory)** *(string)*
The name of the overall project. This field is used by Flowman for sharing mappings and relations between different 
projects. 

* `version` **(optional)** *(string)*
The version currently is not used by Flowman, but can be used for the end-user to help keeping track of which version 
of a project is currently being used.

* `description` **(optional)** *(string)*
A description of the overall project. Can be any text, is not used by Flowman otherwise

* `modules` **(mandatory)** *(list:string)*
The `modules` secion contains a list of *subdirectories* or *filenames* where Flowman should search for more YAML 
specification files. This helps to organize complex projects into different modules and/or aspects. The directory and 
file names are relative to the project file itself.

* `imports` **(optional)** *(list:import)*
Within the `imports` section you can specify different projects to be imported and made available for referencing
its entities.


## Proposed Directory Layout

It is best practice to use a directory structure. Depending on the project, two slightly different approaches have
turned out to be useful: Either separating models and mappings or putting them together.
```
root
 ├── config
 │   ├── environment.yml
 │   ├── connections.yml
 │   └── profiles.yml
 ├── job
 │   ├── job.yml
 │   ├── target-1.yml
 │   │   ...
 │   └── target-n.yml
 ├── schema
 │   ├── schema-1.yml
 │   │   ...
 │   └── schema-n.yml
 ├── macros
 │   ├── macro-1.yml
 │   │   ...
 │   └── macro-n.yml
 ├── relation
 │   ├── relation-1.yml
 │   │   ...
 │   └── relation-n.yml
 ├── mapping
 │   ├── mapping-1.yml
 │   │   ...
 │   └── mapping-n.yml
 └── project.yml
```
