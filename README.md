# Flowman

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/dimajix/flowman.svg?branch=develop)](https://travis-ci.org/dimajix/flowman)
[![Documentation](https://readthedocs.org/projects/flowman/badge/?version=latest)](https://flowman.readthedocs.io/en/latest/)

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


## Documentation

You can find comprehensive documentation at [Read the Docs](https://flowman.readthedocs.io/en/latest/). 


# Installation

You can either grab an appropriate pre-build package at https://github.com/dimajix/flowman/releases or you
can build your own version via Maven with

    mvn clean install
    
Please also read [BUILDING.md](BUILDING.md) for detailed instructions, specifically on build profiles.


## Installing the Packed Distribution 

The packed distribution file is called `flowman-{version}-bin.tar.gz` and can be extracted at any 
location using

    tar xvzf flowman-{version}-bin.tar.gz


# Command Line Utils

The primary tool provided by Flowman is called `flowexec` and is locaed in the `bin` folder of the 
installation directory.

## General Usage

The `flowexec` tool has several subcommands for working with objects and projects. The general pattern 
looks as follows

    flowexec [generic options] <cmd> <subcommand> [specific options and arguments]

For working with `flowexec`, either your current working directory needs to contain a Flowman
project with a file `project.yml` or you need to specify the path to a valid project via

    flowexec -f /path/to/project/folder <cmd>
    
## Interactive Shell

With version 0.14.0, Flowman also introduced a new interactive shell for executing data flows. The shell can be
started via

    flowshell -f <project>
    
Within the shell, you can interactively build targets and inspect intermediate mappings.
