# Flowman

Flowman is a Spark based ETL tool.

# Installation

The Maven build will create both a packed distribution file and a Docker image.

    mvn clean install -PCDH-5.15

## Installing the Packed Distribution 

The packed distribution file is called `flowman-{version}-bin.tar.gz` and can be extracted at any 
location using

    tar xvzf flowman-{version}-bin.tar.gz

# Command Line Util

The primary tool provided by Flowman is called `flowexec` and is locaed in the `bin` folder of the 
installation directory.

## General Usage

The `flowexec` tool has several subcommands for working with objects and projects. The general pattern 
looks as follows

    flowexec [generic options] <cmd> <subcommand> [specific options and arguments]

For working with `flowexec`, either your current working directory needs to contain a Flowman
project with a file `project.yml` or you need to specify the path to a valid project via

    flowexec -f /path/to/project/folder <cmd>
    
    
# Debugging

When you want to run the application inside your IDE (for example for debugging purpose), the best way to do that is
to actually install (via `tar xvzf ...`) the application into some directory and then set the environment variable
`FLOWMAN_HOME` accordingly. This will ensure that all plugins can be found and loaded.
