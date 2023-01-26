# [![](images/flowman-favicon.png) Flowman](https://flowman.io)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/dimajix/flowman.svg?branch=develop)](https://travis-ci.org/dimajix/flowman)
[![Documentation](https://readthedocs.org/projects/flowman/badge/?version=latest)](https://flowman.readthedocs.io/en/latest/)

## What is Flowman

Flowman is a Spark based *data build tool* that simplifies the act of writing data transformation application. Flowman
can be seen as a ETL tool, with a strong focus on transformation and schema management. 

The main idea is that developers define all input/output tables and the whole transformation logic in purely 
declarative YAML files instead of writing complex Spark jobs in Scala or Python. The main advantage of this approach 
is that many technical details of a correct and robust implementation are encapsulated and the user can concentrate on 
the data transformations themselves.

In addition to writing and executing data transformations, Flowman can also be used for managing physical data models, 
i.e. Hive tables but also JDBC tables. Flowman will create such tables from a specification with the correct schema, 
and Flowman also provides mechanisms to automatically migrate these tables when the schema changes due to updated
transformation logic (i.e. new columns are added, data types are changed, etc).

This helps to keep all aspects (like transformations and schema information) in a single place managed by a single 
application.

### Use Cases

Flowman suits well to the requirements of a modern Big Data stack serving multiple different purposes like reporting,
analytics, ML and more. Building on Sparks ability to integrate different data sources, Flowman will serve as the
central place in your value chain for data preparations for the next steps.

[![Flowman Logo](images/flowman-overview.png)](https://flowman.io)


### Notable Features

* Declarative syntax in [YAML files](spec/index.md)
* Full lifecycle management of data models (create, migrate and destroy Hive tables, JDBC tables or file based storage)
* Flexible expression language
* Jobs for managing build targets (like copying files or uploading data via sftp)
* Automatic dependency analysis to build targets in the correct order
* Powerful yet simple [command line tool for batch execution](cli/flowexec/index.md)
* Powerful [Command line tool for interactive data flow analysis](cli/flowshell/index.md)
* [History server](cli/history-server.md) that provides an overview of past jobs and targets including lineage
* [Metric system](cookbook/execution-metrics.md) with the ability to publish these to servers like Prometheus
* Extendable via Plugins


## Where to go from here

### Quickstart & Tutorial
A small [quickstart guide](quickstart.md) will lead you through a simple example. 


```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :caption: Quickstart Guide

   quickstart
```   


### Core Concepts

In order to successfully implement your projects with Flowman, you need to get a basic understanding of Flowmans 
[core concepts](concepts/index.md) and abstractions.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :caption: Concepts

   concepts/index
   concepts/*
```   


### Tutorial

After you have finished the introduction, you may want to proceed with the [Flowman tutorial](tutorial/index.md) to get 
more in-depth knowledge step by step.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :caption: Tutorial

   tutorial/index
   tutorial/*
```   


### CLI Documentation

Flowman provides a command line utility (CLI) for running flows. Details are described in the
following sections:

* [Flowman Executor](cli/flowexec/index.md): Documentation of the Flowman Executor CLI
* [Flowman Shell](cli/flowshell/index.md): Documentation of the Flowman Shell CLI
* [Flowman Server](cli/history-server.md): Documentation of the Flowman Server CLI

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :titlesonly:
   :caption: Command Line Tools
   
   cli/index
   cli/flowexec/index
   cli/flowshell/index
   cli/*
```   


### Specification Documentation

So-called specifications describe the logical data flow, data sources and more. A full
specification contains multiple entities like mappings, data models and jobs to be executed.
More detail on all these items is described in the following sections:

* [Specification Overview](spec/index.md): An introduction for writing new flows
* [Mappings](spec/mapping/index.md): Documentation of available data transformations
* [Relations](spec/relation/index.md): Documentation of available data sources and sinks
* [Targets](spec/target/index.md): Documentation of available build targets
* [Schema](spec/schema/index.md): Documentation of available schema descriptions
* [Jobs](spec/job/index.md): Documentation of creating jobs and building targets

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :titlesonly:
   :caption: Reference Documentation
   
   spec/index
   spec/project
   spec/module
   spec/mapping/index
   spec/relation/index
   spec/target/index
   spec/job/index
   spec/dataset/index
   spec/schema/index
   spec/connection/index
   spec/test/index
   spec/assertion/index
   spec/measure/index
   spec/template/index
   spec/metric/index
   spec/hooks/index
   spec/namespace
   spec/fields
   spec/expressions
   spec/profiles
```


### Testing & Documenting

* [Testing](testing/index.md) How to implement tests in Flowman
* [Documenting](documenting/index.md) How to create detailed documentation of your Flowman project

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :caption: Testing
   
   testing/index
   testing/*
```

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :caption: Documenting
   
   documenting/index
   documenting/*
```

### Workflow

Flowman provides simple but powerful command line tools for executing your projects. This provides a great flexibility
for defining your perfect workflow from development to production deployment. But this flexibility makes it more 
difficult to get started. Luckily Flowman also provides some guidance for setting up your projects and for defining
a streamlined development workflow.

* [Development & Deployment Workflow](workflow/index.md): How to implement an efficient development workflow with Flowman


```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :caption: Workflow
   
   workflow/index
   workflow/*
```


### Installation & Configuration
* [Flowman Installation](setup/installation.md): Installation guide for local installation
* [Running in Docker](setup/docker.md): How to run Flowman in Docker
* [Configuration](setup/config.md): Configuration settings

```eval_rst
.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Setup & Configuration
   
   setup/index
   setup/installation
   setup/docker
   setup/windows
   setup/config
   setup/building
```


### Cookbooks

Cookbooks contain some "how-to"s for frequent issues, like working with [Kerberos authentication](cookbook/kerberos.md), 
integrating [automatic Impala catalog updates](cookbook/impala.md) or [overriding JARs](cookbook/override-jars.md) and
much more.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :caption: Cookbooks
   
   cookbook/*
```


### Connectors & Plugins

Flowman also provides optional plugins which extend functionality. You can find an overview of all
[available plugins](plugins/index.md)

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :titlesonly:
   :caption: Connectors & Plugins
   
   connectors/index
   plugins/index
```


```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:
   :hidden:
   :caption: Misc
   
   faq/index
   releases
```

