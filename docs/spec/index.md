# Project Specification

Flowman uses so called *flow specifications* which contain all details of data transformations
and additional processing steps. Flow specifications are provided by the user as (potentially 
multiple) YAML files using a purely declarative approach.  

In addition to data flows, these specifications also include information about data sources 
and sinks, schema information, credentials if required and complete workflows using jobs.

The top level object in a Flowman specification always is a [*Project*](project.md). Each
project contains the full specification of all data transformation and processing steps and
can be executed independently.

All specification files are written as declarations in YAML syntax, which provides easy 
readability. These YAML files are read in by Flowman executables and data flows are 
executed as specified on the command line (more on that in [Flowman CLI](../cli/flowexec.md))

## Project Documentation
* [Project](project.md)
* [Module](module.md)
* [Profiles](profiles.md)
* [Namespace](namespace.md)


## Entity Documentation

Flowman has a couple of different main entities, which are documented seperately:

* [Mappings](mapping/index.md): Data transformations
* [Relations](relation/index.md): Data sources and sinks
* [Targets](target/index.md): Build targets
* [Schema](schema/index.md): Schema descriptions
* [Connections](connection/index.md): Connection specifications
* [Jobs](job/index.md): Build jobs
* [Datasets](dataset/index.md): Datasets
* [Metrics](metric/index.md): Publishing metrics
* [Hooks](hooks/index.md): Execution hooks


## Sub Pages
```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
