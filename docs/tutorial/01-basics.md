# Lesson 1 - Flowman Basics

Welcome to the first lesson for learning how to develop data transformation pipelines with Flowman. This first lesson
will guide you through the basic core concepts of Flowman and will learn how to use the Flowman command line tools.
All lessons will use a subset of a publicly available data set about weather data. This first lesson will only focus
on the weather stations metadata and perform a technical format conversion from CSV to Parquet.

## 1. What to Expect

### Objectives
* You will understand Flowman's philosophy and development workflow
* You will get known to the main entities in Flowman which make up a project
* You will learn how to use Flowman's command line tool to execute a project

You can find the full source code of this lesson [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/develop/lessons/01-basics) 

### Description
As described above, this first lesson will perform a technical format conversion. We will read in a CSV file from
S3 containing the list of all available weather stations containing attributes like identifiers (WBAN and USAF),
a short name, the geolocation etc. We then will simply write this list into the local file system as Parquet files,
which are optimized for analytical workloads in Big Data environment.

### Processing Steps
So we will perform the following steps:
1. Read in weather station metadata from CSV file from S3
2. Write weather station metadata as Parquet files into local file system


## 2. Core Concepts

Flowman is a sophisticated tool to create data transformation applications using a declarative syntax. Although
being based on Apache Spark, Flowman will take care of many low level technical details, so you do not need to be
an expert for Apache Spark to be able to successfully implement projects with Flowman.

Flowman adds a layer of abstraction on top of Apache Spark and provides a consistent and flexible entity model for
describing all data sources, data sinks and data transformations between them.


## 3. Project

First Flowman uses the term *project* to bundle multiple data sources, sinks and transformations into a single package.
As we will see, you can have multiple data transformation pipelines in a single project.

### 3.1 project.yml

The entry point of each Flowman project is a `project.yml` file, which contains project metadata like a project name,
an optional project version. The main information in a `project.yml` file simply is a list of subdirectories containing
the source code as additional YAML files containing all entity descriptions. The YAML files inside subdirectories will 
be read by Flowman and deserialized to a logical entity model. 

It is completely up to you how you want to organize all YAML files in subdirectories. In this tutorial, we simply
use separate subdirectories for different entity types. For example all *relations* (which describe data sources and
data sinks) will be stored in a `model` subdirectory, all mappings (which describe the data transformations) are stored
in a `mapping` subdirectory and so on.

```yaml
# The project name is mandatory
name: "weather"
# Optionally you can provide a project version
version: "1.0"
# Optionally you can provide a description
description: "
    This is a simple but very comprehensive example project for Flowman using publicly available weather data.
    The project will demonstrate many features of Flowman, like reading and writing data, performing data transformations,
    joining, filtering and aggregations. The project will also create a meaningful documentation containing data quality
    tests.
  "

# The following modules simply contain a list of subdirectories containing YAML files containing all entities
modules:
  # All relations describing data sources and sinks are stored in the "model" subdirectory 
  - model
  # Data transformations (mappings) are stored in the "mapping" subdirectory
  - mapping
  # Build targets go into the "target" directory
  - target
  # Build jobs bundling multiple targets go into the "job" directory
  - job
  # All configurations go into the "config" directory
  - config
```

### 3.2 Configuration & Environment

#### Configuration
Apache Spark and Flowman might require some configurations, for example access keys to S3, proxy settings and so on.
These are set in a `config` section, and are best kept in a central file. These Spark/Hadoop specific configuration 
properties are documented on the Spark homepage, while additional Flowman specific settings are documented in
the Flowman documentation.

In this example we will access data stored in Amazon S3, so we need to provide some configurations like an 
anonymous credential provider, proxy settings and so on. In this example all these configurations settings are stored
in the file `config/aws.yml`, but you can provide multiple `config` sections in multiple files.

```yaml
config:
  # Use anonymous access to S3
  - spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
  # Inject proxy for accessing S3
  - spark.hadoop.fs.s3a.proxy.host=$System.getenv('S3_PROXY_HOST', $System.getenv('AWS_PROXY_HOST'))
  - spark.hadoop.fs.s3a.proxy.port=$System.getenv('S3_PROXY_PORT', $System.getenv('AWS_PROXY_PORT' ,'-1'))
  - spark.hadoop.fs.s3a.proxy.username=
  - spark.hadoop.fs.s3a.proxy.password=
  - spark.hadoop.fs.s3a.endpoint=s3.eu-central-1.amazonaws.com
  - spark.hadoop.fs.s3a.signature_version=s3v4
```

#### Environment
Flowman also supports so-called *environment variables* which can be used in the data flow specifications and can
be replaced by different values on the command line. In this lesson we use an environment variable `basedir` for
specifying the output location of transformation results in a file system. We store all these variables in
`config/environment.yml`, again you can have multiple `environment` sections in different files:

```yaml
environment:
  - basedir=file:///tmp/weather
```


### 3.3 Relations

Now that we have discussed the more technical and boring aspect of providing configuration settings and a single
environment variable, we will now provide two *relations*. Each relation represents a physical storage, for example
a directory containing CSV files, a relation SQL database etc. This lesson stores all relations within the `model`
subdirectory.

#### Source Relation
First we specify the source relation pointing to the raw CSV files on S3 containing the weather stations metadata.
We create a `stations_raw` relation in the `relations` section inside the file `model/stations-raw.yml`. Each
relation (as most other Flowman entities) has a `kind` which defines the type of the entity. In this case we use
the `file` kind to create a relation containing files. All other properties defined in the `stations_raw` relation
are specific for the `file` kind.

The [Flowman documentation](https://flowman.readthedocs.io/en/latest/spec/relation/index.html#relation-types) contains 
many other relation types, each with a different `kind` and specific properties.

```yaml
relations:
  stations_raw:
    # The relation is of type "file"
    kind: file
    # ... and it uses CSV as file format
    format: csv
    # data itself is stored at the following location
    location: "s3a://dimajix-training/data/weather/isd-history/"
    # Specify some CSV-specific options
    options:
      sep: ","
      encoding: "UTF-8"
      quote: "\""
      header: "true"
      dateFormat: "yyyyMMdd"
```
Note that we did not specify any schema definition (i.e. which columns are contained in the CSV file). Instead, we will 
let Flowman use the column names from the header line in the CSV file. In the next lesson we will learn a better
alternative to explicitly specify a schema.

#### Target Relation
We also need to specify a relation to hold the data written by Flowman. Again we use a `file` relation, but this time
using Parquet as the file format. Also note that the location is using the `basedir` environment variable which has been
defined the `config/environment.yml` file. This way we can easily change the location of the output files either
by changing the variable definition directly inside the file, or - more elegantly - via a command line parameter at
execution time.
```yaml
relations:
  stations:
    kind: file
    description: "The 'stations' table contains meta data on all weather stations"
    format: parquet
    location: "$basedir/stations/"
```

Note that both the source and target locations actually are defined in a generic way and do not contain any information
about being used as a data source or data sink. Flowman can use every relation as a source or target or even both 
within a single project, as we will see in a later lesson.


### 3.4 Mappings

Now we only have defined a source and a target relation. In the next step we need to create a data flow for
connecting them. First we define a *mapping*, which simply reads in the data from the source relation. This mapping
is defined in the `mapping/stations.yml` file in the `mappings` section:

```yaml
mappings:
  # This mapping refers to the "raw" relation and reads in data from the source in S3
  stations_raw:
    kind: relation
    relation: stations_raw
```

Similar to relations, each mapping has a `kind` which describes the transformation type. The `read` mapping actually
is not a real transformation, but the entry point of a data flow. We don't do any additional data processing in this
lesson, so we don't require any additional mapping. You can find an overview of available mappings in the
[Flowman documentation](https://flowman.readthedocs.io/en/latest/spec/mapping/index.html#mapping-types).

In this first lesson we also need to define a second mapping in order to rename the incoming column names. The original
column names are taken from the CSV file and some of them contain problematic characters like space and braces. We
can easily adjust the columns by adding a `select` mapping:

```yaml
mappings:
  stations_conformed:
    # Specify the mapping kind  
    kind: select
    # Specify the mapping which serve as the input to this one
    input: stations_raw
    # Now list all output columns with SQL expressions which refer to the columns of the input mapping
    columns:
      usaf: "USAF"
      wban: "WBAN"
      name: "`STATION NAME`"
      country: "CTRY"
      state: "STATE"
      icao: "ICAO"
      latitude: "CAST(LAT AS FLOAT)"
      longitude: "CAST(LON AS FLOAT)"
      elevation: "CAST(`ELEV(M)` AS FLOAT)"
      date_begin: "CAST(BEGIN AS DATE)"
      date_end: "CAST(END AS DATE)"
```
We will learn a better approach how to provide an appropriate data schema in the next lesson, though.


### 3.5 Targets & Jobs
So far we have defined a source relation, a target relation and a trivial data flow which simply reads from the source
relation. 

#### Build Targets
Now we have to connect the result of the `stations_raw` mapping with the target relation. In Flowman this
connection is established by creating an appropriate *build target*. Each build target represents actual work that
needs to be performed. Again, Flowman implements different 
[build target types](https://flowman.readthedocs.io/en/latest/spec/target/index.html#target-types). The most
important target is the [`relation`](https://flowman.readthedocs.io/en/latest/spec/target/relation.html) target
which stores the result records of a mapping into a relation. 

The file `target/stations.yml` if this example defines a single build target called `stations` as follows: 
```yaml
targets:
  # Define a build target "stations"...
  stations:
    # ... which builds a relation
    kind: relation
    # ... by reading the result from the mapping "stations_conformed"
    mapping: stations_conformed
    # ... and by writing the records to the relation "stations"
    relation: stations
```

#### Jobs
Now we are almost finished with the first example, and actually this example could already be run by executing the
build target above. But let's add one more entity, called *job*. At first glance, a job simply is a collection of
build targets, which should be executed together. Even if this example only contains a single target, it is still
advisable to create a job, since this will provide some more features which we will discuss in later lessons.

The job of this lesson is defined in the file `job/main.yml`:

```yaml
jobs:
  # Define the 'main' job, which implicitly is used whenever you build the whole project
  main:
    # List all targets which should be built as part of the `main` job
    targets:
      - stations
```


### 3.6 Summary

The preceding steps built a complete data flow which looks as follows:

```{mermaid}
  flowchart LR
    r_stations_raw[[relation_raw\n stations\n CSV\n S3]] --> m_stations_raw
    m_stations_raw{{mapping\n stations_raw}} --> m_stations_conformed
    m_stations_conformed{{mapping\n stations_conformed}} --> t_stations
    t_stations(target\n stations) --> r_stations[[relation\n stations\n Parquet\n local]]
    style t_stations fill:#93f,stroke-width:3px,color:#fff
    style r_stations_raw fill:#339,stroke-width:3px,color:#fff
    style m_stations_raw fill:#669,stroke-width:3px,color:#fff
    style m_stations_conformed fill:#669,stroke-width:3px,color:#fff
    style r_stations fill:#339,stroke-width:3px,color:#fff
```


## 4. Execution

We now have defined a complete Flowman project including configuration, environment variables, relations, mappings,
targets and a job. Now we want to execute this project by using the `flowexec` command line tool.

The general pattern for building all targets
```shell
flowexec -f <project_dirctory> job build <job_name>
```

In our example this means:
```shell
flowexec -f lessons/01-basics job build main
```

### 4.1 Overriding Environment Variables
You can also redefine any environment variable on the command line by using the `-D <variable>=<value>` command line
option:
```shell
flowexec -f lessons/01-basics job build main -D basedir=/tmp/flowman
```
Of course, you can use multiple parameters `-D` for defining different variables.

### 4.2 Dirty Targets
Flowman will only execute a target when it is considered to be *dirty*. For `relation` targets this means that the
relation (or the partition) does not contain any data. Otherwise, Flowman will skip the execution of the target. You
can see this feature when you execute that job a second time. In this case Flowman will see that the target relation
already contains data and therefore won't automatically overwrite the data.

But of course you can always force execution by adding a `--force` parameter to the command line:
```shell
flowexec -f lessons/01-basics job build main --force
```


## 5. Next Lessons
In the next lessons, we will have a closer look at explicit schema definitions, more complex transformations than only 
reading data, and we will also learn how to use job parameters and the effect of multiple build targets within a single
job.
