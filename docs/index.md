# Flowman Introduction

## What is Flowman

Flowman is a Spark based ETL program that simplifies the act of writing data transformations.
The main idea is that users write so called *specifications* in purely declarative YAML files
instead of writing Spark jobs in Scala or Python. The main advantage of this approach is that
many technical details of a correct and robust implementation are encapsulated and the user
can concentrate on the data transformations themselves.

In addition to writing and executing data transformations, Flowman can also be used for
managing physical data models, i.e. Hive tables. Flowman can create such tables from a
specification with the correct schema. This helps to keep all aspects (like transformations
and schema information) in a single place managed by a single program.

### Noteable Features

* Declarative syntax in YAML files
* Data model management (Create and Destroy Hive tables or file based storage)
* Flexible expression language
* Jobs for managing build targets (like copying files or uploading data via sftp)
* Powerful yet simple command line tool
* Extendable via Plugins


## Where to go from here

### Installation
* [Flowman Installation](installation.md): Installation Guide


### CLI Documentation

Flowman provides a command line utility (CLI) for running flows. Details are described in the
following sections:

* [Flowman Executor](cli/flowexec.md): Documentation of the Flowman Executor CLI
* [Flowman Server](cli/flowserver.md): Documentation of the Flowman Server CLI


### Specification Documentation

So called specifications describe the logical data flow, data sources and more. A full
specification contains multiple entities like mappings, data models and jobs to be executed.
More detail on all these items is described in the following sections:

* [Specification Overview](spec/index.md): An introduction for writing new flows
* [Mappings](spec/mapping/index.md): Documentation of available data transformations
* [Relations](spec/relation/index.md): Documentation of available data sources and sinks
* [Targets](spec/target/index.md): Documentation of available build targets
* [Schema](spec/schema/index.md): Documentation of available schema descriptions
* [Jobs](spec/job/index.md): Documentation of creating jobs and building targets


### Cookbooks

* [Testing](cookbook/testing.md) How to implement tests in Flowman


## Table of Contents

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   introduction
   installation
   lifecycle
   spec/index
   spec/relation/index
   spec/mapping/index
   spec/target/index
   spec/job/index
   spec/dataset/index
   spec/schema/index
   spec/connection/index
   spec/metric/index
   cookbook/index
```
