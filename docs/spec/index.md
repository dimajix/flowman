---
layout: page
title: Flowman Specifications Overview
permalink: /spec/index.html
---
# Flowman Flow Specification

Flowman uses so called *flow specifications* which contain all details of data transformations
and additional processing steps. Flow specifications are provided by the user as (potentially 
multiple) YAML files using a purely declarative approach.  

In addition to data flows, these specifications also include information about data sources 
and sinks, schema information, credentials if required and complete workflows using jobs.

The top level object in a Flowman specification always is a [*Project*](projects.html). Each
project contains the full specification of all data transformation and processing steps and
can be executed independently.

All specification files are written as declarations in YAML syntax, which provides easy 
readability. These YAML files are read in by Flowman executables and data flows are 
executed as specified on the command line (more on that in [Flowman CLI](../cli/flowexec.html))

## Entity Documentation

Flowman has a couple of different main entities, which are documented seperately:

* [Mappings](mapping/index.html): Documentation of available data transformations
* [Relations](relation/index.html): Documentation of available data sources and sinks
* [Targets](target/index.html): Documentation of available output targets
* [Schema](schema/index.html): Documentation of available schema descriptions
* [Connections](connection/index.html): Documentation of connection specifications
* [Jobs & Tasks](job/index.html): Documentation of creating jobs and executing tasks
* [Datasets](dataset/index.html): Documentation of using datasets
* [Metrics](metric/index.html): Documentation of publishing metrics

