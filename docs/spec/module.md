# Modules

Flowman YAML specifications can be split up into an arbitrary number of files. From a project
perspective these files form *modules*, and the collection of all modules create a *project*.

Modules (either as individual files or as directories) are specified in the 
[project main file](project.md)

Each module supports the following top level entries:
```yaml
config:
    ...
    
environment:
    ...

profiles:
    ...

relations:
    ...
    
connections:
    ...
    
mappings:   
    ...

targets:
    ...

tests:
    ...

templates:
    ...

jobs:
    ...
```
Each top level entry may appear at most once in every file, but multiple files can have the same top level entries. 
This again helps to split up the whole specifications into multiple files in order to help organizing your data flow.


## Module Sections

As explained above, each file belonging to a module can contain multiple sections. The meaning
and contents of each section are explained below


### `config` Section

The `config` section contains a list of Hadoop, Spark or Flowman configuration properties, for example

```yaml
config:
  - spark.hadoop.fs.s3a.endpoint=s3.eu-central-1.amazonaws.com
  - spark.hadoop.fs.s3a.access.key=$System.getenv('AWS_ACCESS_KEY_ID')
  - spark.hadoop.fs.s3a.secret.key=$System.getenv('AWS_SECRET_ACCESS_KEY')
  - spark.hadoop.fs.s3a.proxy.host=$System.getenv('S3_PROXY_HOST', $System.getenv('AWS_PROXY_HOST'))
  - spark.hadoop.fs.s3a.proxy.port=$System.getenv('S3_PROXY_PORT', $System.getenv('AWS_PROXY_PORT' ,'-1'))
  - spark.hadoop.fs.s3a.proxy.username=
  - spark.hadoop.fs.s3a.proxy.password=
```

As you can see, each property has to be specified as `key=value`. Configuration properties are evaluated in the order 
they are specified within a single file. 

All Spark config properties are passed to Spark when the Spark session is created. As you can also see, you can use 
[*expression evaluation*](expressions.md) in the values. It is not possible to use expressions for the keys. 


### `environment` Section

The `environment` section contains key-value-pairs which can be accessed via [*expression evaluation*](expressions.md) 
in almost any value definition in the specification files. A typical `environment`section may look as follows
```yaml
environment:
  - start_year=2007
  - end_year=2014
  - export_location=hdfs://export/weather-data
```
All values specified in the environment can be overriden either by [profiles](profiles.md) or by explicitly setting 
them as property definitions on the [command line](../cli/flowexec/index.md).

Note the difference between `environment` and `config`. While the first provides user defined variables to be used
as placeholders in the specification, all entries in `config` impact the execution and are used either directly by
Flowman or by its underlying libraries like Hadoop or Spark.


### `profiles` Section

TBD.


### `relations` Section

The `relations` section simply contains a map of named relations. For example
```yaml
relations:
  measurements-raw:
    kind: file
    format: text
    location: "s3a://dimajix-training/data/weather/"
    pattern: "${year}"
    schema:
      kind: inline
      fields:
        - name: raw_data
          type: string
          description: "Raw measurement data"
    partitions:
      - name: year
        type: integer
        granularity: 1
```
This will define a relation called `measurement-raw` which can be accessed from other elements
like mappings (for reading from the relation) or output operation (for writing to the relation).
The list and syntax of available relations is described in detail in the 
[Relations](relation/index.md) documentation.


### `connections` Section

Similar to `relations` the `connections` section contains a map of named connections. For
example
```yaml
connections:
  my-sftp-server:
    kind: sftp
    host: "${sftp_host}"
    port: ${sftp_port}
    username: "${sftp_username}"
    password: "${sftp_password}"
    keyFile: "${sftp_keyfile}"
    knownHosts: "$System.getProperty('user.home')/.ssh/known_hosts"
```
This will declare a connection called `my-sftp-server` of kind `sftp` which referenced in
specific mappings or tasks (for example inside a sftp upload task). Detailed descriptions 
of all supported connections is provided in the [Connections](connection/index.md) 
documentation.


### `mappings` Section

Again the `mappings` section contains named mappings which describe the data flow and any
data transformation. For example
```yaml
mappings:
  measurements-raw:
    kind: read-relation
    source: measurements-raw
    partitions:
      year:
        start: $start_year
        end: $end_year
    columns:
      raw_data: String
```
This defines a mapping called `measurements-raw` and reads data from a relation called
`measurements-raw`. As you can see, you can reuse the same name inside different sections,
for example you can use the same name `measurements-raw` as a relation, a mapping and an
output.

You can read all about mappings in the [Mappings](mapping/index.md) section.
 

### `targets` Section

The `targets` section contains a map of named output operations like writing to files, 
relations or simply dumping the contents of a mapping on the console. For example
```yaml
targets:
  measurements-dump:
    kind: dump
    enabled: false
    input: measurements
    limit: 100
``` 
This would define one output called `measurements-dump` which will show the first 100 records
from a mapping called `measurements`.

You can read all about build targets in the [Targets](target/index.md) section.


### `tests` Section
Flowman also provides a built in test framework for creating unit tests for your logic. The test framework is able
to replace relations and mappings by mocked data, so the tests do not require any external data sources.


### `jobs` Section

Finally there is the `jobs` section which contains one or multiple named job specifications,
which contain lists of tasks to be executed. Jobs sit one layer above the data flow itself,
they are used to build complex processing pipelines which may also require additional
actions like uploading files via SFTP.

A typical job specification may look as follows:
```yaml
jobs:
  main:
    description: "Main job"
    tasks:
      - kind: show-environment
      - kind: print
        text:
          - "project.name=${project.name}"
          - "project.version=${project.version}"
          - "project.basedir=${project.basedir}"
          - "project.filename=${project.filename}"
      - kind: call
        job: dump-all
        force: true
```  
This would create a single job called `main` which contains three tasks which are executed
sequentially (first task would show all environment variables, second would print some
information on the console and the last would call another job called `dump-all`).

Every project should contain one job called `main` which is executed whenever the whole
project is to be executed using the [Flowman CLI](../cli/flowexec/index.md)


### `templates` Section

With Flowman 0.18.0, a new templating mechanism is implemented which helps you to avoid repeating similar
specification blocks.
