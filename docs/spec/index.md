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


Flowman has a couple of different main entities, which are documented seperately:

```eval_rst
.. toctree::
   :maxdepth: 1

   namespace
   project
   module

   mapping/index
   relation/index
   target/index
   job/index
   dataset/index
   schema/index
   connection/index
   test/index
   metric/index
   hooks/index
```


## Misc Topics
```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   expressions
   fields
   profiles
```
