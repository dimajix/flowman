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

* [Mappings](mapping/index.md): Documentation of available data transformations
* [Relations](relation/index.md): Documentation of available data sources and sinks
* [Targets](target/index.md): Documentation of available build targets
* [Schema](schema/index.md): Documentation of available schema descriptions
* [Connections](connection/index.md): Documentation of connection specifications
* [Jobs](job/index.md): Documentation of creating jobs
* [Datasets](dataset/index.md): Documentation of using datasets
* [Metrics](metric/index.md): Documentation of publishing metrics
* [Hooks](hooks/index.md): Documentation of hooks


## Misc Documentation
* [Fields](fields.md)
* [Expressions](expressions.md)

