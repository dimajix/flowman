# Flowman Flow Specification

Flowman uses so called *flow specifications* which contain all details of data transformations
and additional processing steps. These specifications also include information about data sources
and sinks, schema information and credentials if required.

The top level object in a Flowman specification always is a [*Project*](projects.md). Each
project contains the full specification of all data transformation and processing steps and
can be executed independently.

All specification files are written as declarations in YAML syntax, which provides easy 
readability. These YAML files are read in by Flowman executables and data flows are 
executed as specified on the command line (more on that in [Flowman CLI](../cli/flowexec.md))

## Entity Documentation

Flowman has a couple of different main entities, which are documented seperately:

* [Mappings](mapping/index.md): Documentation of available data transformations
* [Relations](relation/index.md): Documentation of available data sources and sinks
* [Outputs](output/index.md): Documentation of available output operations
* [Schema](schema/index.md): Documentation of available schema descriptions
* [Connections](connection/index.md): Documentation of connection specifications
* [Jobs & Tasks](job/index.md): Documentation of creating jobs and executing tasks

