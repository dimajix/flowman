# Lesson 2 - Schema Definitions

In the first lesson we saw a simple example for reading and writing data in order to perform a technical format
conversion from CSV to Parquet. But we also saw some issues, especially with CSV files, which do not contain a precise
schema information. This lesson will present a remedy to improve the situation by explicitly providing a schema 
definition in each relation.

## 1. What to Expect

### Objectives
* You will understand the benefits of explicitly providing schema definitions
* You will learn how to provide a schema definition

You can find the full source code of this lesson [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/develop/lessons/02-schema)

### Description
Similar to the first lesson, we will read in a CSV file from S3 containing the list of all available weather stations 
containing attributes like identifiers (WBAN and USAF), a short name, the geolocation etc. We then will simply write 
this list into the local file system as Parquet files, which are optimized for analytical workloads in Big Data 
environment.

### Processing Steps
So we will perform the following steps:
1. Read in weather station metadata from CSV file from S3
2. Write weather station metadata as Parquet files into local file system


## 2. Implementation

### 2.1 Relations
The only change required for the Flowman project is to provide schema definitions - for input and output relations.  

#### Source Relation
Most relations support a `schema` property, which contains the description of the data, i.e. the column names and data
types. Flowman supports [different schema kinds](https://flowman.readthedocs.io/en/latest/spec/schema/index.html#schema-types) 
as well, in our example we use an externally stored Avro schema definition file for the source schema definition. The
definition of the `stations_raw` relation then changes as follows:

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
    # Specify the schema (which is stored in an external file)
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
```

Note that the schema definition uses the pre-defined environment variable `project.basedir` to specify the location
of the Avro schema definition file inside the project directory.

#### Target Relation
We also provide a schema definition for the target relation. Strictly speaking, this is not required in many cases,
since in the corresponding build target Flowman will simply write records with the schema of the mapping being stored
in a relation. Nevertheless, in case the target relation is part of an interface that other consumers rely on, it might
be a good idea to explicitly specify a schema. Moreover, in a later lesson we will see that schema information is very
relevant for creating independent test cases.

The following example uses an embedded schema definition instead of an external file. 
```yaml
relations:
  stations:
    kind: file
    description: "The 'stations' table contains meta data on all weather stations"
    format: parquet
    location: "$basedir/stations/"
    schema:
      kind: inline
      fields:
        - name: "usaf"
          type: string
        - name: "wban"
          type: string
        - name: "name"
          type: string
        - name: "country"
          type: string
        - name: "state"
          type: string
        - name: "icao"
          type: string
        - name: "latitude"
          type: float
        - name: "longitude"
          type: float
        - name: "elevation"
          type: float
        - name: "date_begin"
          type: date
        - name: "date_end"
          type: date
```

### 2.2 Mappings
Now since an explicit schema is provided with the CSV source relation with appropriate column names and data types,
there is no need anymore to transform the incoming columns within a `select` mapping. Therefore, we can completely
remove the `stations_conformed` mapping.


### 2.3 Targets
Since we removed the `stations_conformed` mapping, we also need to adopt the definition of the `stations` target
in the file `target/stations.yml` accordingly and replace the mapping with the `stations_raw` mapping:

```yaml
targets:
  # Define a build target "stations"...
  stations:
    # ... which builds a relation
    kind: relation
    # ... by reading the result from the mapping "stations_raw"
    mapping: stations_raw
    # ... and by writing the records to the relation "stations"
    relation: stations
```


## 3. Execution

The Flowman command line tool `floexec` can also be used to inspect relations. This is what we will do now as a first
step.

### 3.1 Describe Relations
Let us describe the schema of the `stations_raw` relation. You should see a schema which exactly matches the one
defined in the Avro schema definition file.
```shell
cd /home/flowman
flowexec -f lessons/02-schema relation describe stations_raw
```

Let us also describe the schema of the `stations` relation, this will return a schema which matches the embedded
schema in `model/stations.yml`. 
```shell
cd /home/flowman
flowexec -f lessons/02-schema relation describe stations
```


### 3.2 Describe Relations without Schema
Now let us have a look at the schema definitions of the first lesson using the same command line tool. You will see
a different schema definition for the original CSV relation. Column names will be inferred from the header line and
all data types are set to `string`:
```shell
cd /home/flowman
flowexec -f lessons/01-basics relation describe stations_raw
```

If you next try to describe the schema of the output from the first lesson, you will possibly experience an error since
Flowman is not provided an explicit schema and therefore tries to infer the actual schema from files. But if the project
has not been executed yet, there won't be any file to inspect:
```shell
cd /home/flowman
flowexec -f lessons/01-basics relation describe stations
```


### 3.3 Build Target
Finally, you can build the project again using the `job build` command:
```shell
flowexec -f lessons/02-schema job build main --force
```


## 4. Next Lessons
In the next lessons, we will have a closer look at more complex transformations than only reading data, and we will
also learn how to use job parameters and the effect of multiple build targets within a single job.
