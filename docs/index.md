# Flowman Introduction

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/dimajix/flowman.svg?branch=develop)](https://travis-ci.org/dimajix/flowman)
[![Documentation](https://readthedocs.org/projects/flowman/badge/?version=latest)](https://flowman.readthedocs.io/en/latest/)

## What is Flowman

Flowman is a Spark based *data build tool* that simplifies the act of writing data transformation application. Flowman
can be seen as a ETL tool, with a strong focus on transformation and schema management. 

The main idea is that developers write so called *specifications* in purely declarative YAML files instead of writing 
complex Spark jobs in Scala or Python. The main advantage of this approach is that many technical details of a correct 
and robust implementation are encapsulated and the user can concentrate on the data transformations themselves.

In addition to writing and executing data transformations, Flowman can also be used for managing physical data models, 
i.e. Hive tables but also JDBC tables. Flowman can create such tables from a specification with the correct schema, 
and Flowman also provides mechanisms to automatically migrate these tables when the schema changes due to updates
transformation logic (i.e. new columns are added).

This helps to keep all aspects (like transformations and schema information) in a single place managed by a single 
application.

[![Flowman Logo](images/flowman-logo.png)](https://flowman.io)

### Notable Features

* Declarative syntax in [YAML files](spec/index.md)
* Full lifecycle management of data models (create, migrate and destroy Hive tables, JDBC tables or file based storage)
* Flexible expression language
* Jobs for managing build targets (like copying files or uploading data via sftp)
* Automatic dependency analysis to build targets in the correct order
* Powerful yet simple [command line tool for batch execution](cli/flowexec.md)
* Powerful [Command line tool for interactive data flow analysis](cli/flowshell.md)
* [History server](cli/history-server.md) that provides an overview of past jobs and targets including lineage
* [Metric system](cookbook/metrics.md) with the ability to publish these to servers like Prometheus
* Extendable via Plugins


## Where to go from here

## Quickstart & Tutorial
A small [quickstart guide](quickstart.md) will lead you through a simple example. After you have finished the
introduction, you may want to proceed with the [Flowman tutorial](tutorial/index.md) to get more in-depth knowledge
step by step.


### Installation
* [Flowman Installation](setup/installation.md): Installation guide for local installation
* [Running in Docker](setup/docker.md): How to run Flowman in Docker
* [Configuration](setup/config.md): Configuration settings


### CLI Documentation

Flowman provides a command line utility (CLI) for running flows. Details are described in the
following sections:

* [Flowman Executor](cli/flowexec.md): Documentation of the Flowman Executor CLI
* [Flowman Shell](cli/flowshell.md): Documentation of the Flowman Shell CLI
* [Flowman Server](cli/history-server.md): Documentation of the Flowman Server CLI


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

* [Testing](testing/index.md) How to implement tests in Flowman
* [Documenting](documenting/index.md) How to create detailed documentation of your Flowman project


### Plugins

Flowman also provides optional plugins which extend functionality. You can find an overview of all 
[available plugins](plugins/index.md)


## Table of Contents

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   quickstart
   concepts/index
   tutorial/index
   spec/index
   testing/index
   documenting/index
   cli/index
   setup/index
   connectors/index
   plugins/index
   cookbook/index
   releases
```
