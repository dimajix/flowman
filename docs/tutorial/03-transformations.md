# Lesson 3 - Transformations

In this next installment of the Flowman tutorial we will work with a new data set, namely the weather measurements
themselves. These are stored in a proprietary, ASCII based text format. Therefore, we cannot simply use a CSV reader
like we did before, instead we need to parse the lines using SQL `SUBSTR` operations.

## 1. What to Expect

### Objectives
* You will see more non-trivial transformations in action
* You will learn an elegant way to let Flowman automatically generate the correct schema for outgoing relations
* You will learn how to use data partitions

You can find the full source code of this lesson [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/develop/lessons/03-transformations)

### Description
This time we will read in the raw measurement data, which contains many weather measurements per weather stations
(typically one measurement per weather station and per hour, but there might be more or less). We then will store
the extracted measurements in Parquet files again, which are well suited for any downstream analytical workload.

### Processing Steps
So we will perform the following steps:
1. Read in weather measurements as simple text files from S3
2. Extract some measurement attributes via SQL functions
3. Write weather measurements as Parquet files into local file system


## 2. Implementation

Again we start with a project definition file `project.yml`, which looks very similar to the previous ones:
```yaml
name: "weather"
version: "1.0"

# The following modules simply contain a list of subdirectories containing the specification files
modules:
  - model
  - mapping
  - target
  - job
  - config
```

### 2.1 Configuration & Environment
Configuration and environment is still stored in the files `config/aws.yml` and `config/environment.yml`. The only
difference to the previous lesson is the addition of a new environment variable `year`, which will be used to process
only a single year of measurements. Since each year could possibly contain millions of records, a common approach is
to *partition* the data per year and selectively only process a single year.

```yaml
environment:
  - basedir=file:///tmp/weather
  # Define an environment variable to process only a single year
  - year=2013
```

We will learn a small but important improvement how to model this in Flowman in the next lesson instead of using a
simple environment variable. 

### 2.2 Relations
Again we define two relations: One source relation containing the raw weather measurements stored in text file and one 
target relation which contains the extracted and transformed measurements stored as Parquet files. There is one 
important difference to the last lesson, though: This time both the source and the target relation define a so called
*partition column* which represents large chunks of data stored in different directories. In our example, data will
be stored in different partitions for each year of measurement. This physical organization of data enables to 
selectively process only individual years. Also query tools like Hive, Impala or Trino can make use of partition columns
for pruning while directories when the partition column is used in a SQL `WHERE` condition.

#### Source Relation
As explained above, the source data is stored in simple ASCII files, where each record is stored in a separate line.
The records themselves contain a part with fixed locations per attribute and an optional more dynamic part. We solely
focus on the fixed locations, which is far simpler to work with.

But before going into details how to extract the attributes, we define the source relation called `measurements_raw`
as a `file` relation with format `text` (as opposed to the format `csv` which we used before). We also add a logical
*partition column* which enables the organization of all data files into separate subdirectories per year. The 
partition column `year` is mapped to a subdirectory as specified in the `pattern` property. The relation is defined
in the file `model/measurements-raw.yml`.

```yaml
relations:
  measurements_raw:
    kind: file
    format: text
    location: "s3a://dimajix-training/data/weather/"
      # Define the pattern to be used for partitions. The pattern uses the "year" partition column defined below
    pattern: "${year}"
    # Define data partitions. Each year is stored in a separate subdirectory
    partitions:
      - name: year
        type: integer
        granularity: 1
        description: "The year when the measurement was made"
    schema:
      # Specify the (single) column via an embedded schema.
      kind: inline
      fields:
        - name: raw_data
          type: string
          description: "Raw measurement data"
```

#### Target Relation
The relation is defined in the file `model/measurements.yml`. The target relation also contains a partition column
to store data from each year independently in a different directory. In contrast to the source relation, no partition
pattern is specified - Flowman will use a Spark compatible directory layout, which is well understood by many Big Data
tools like Hive, Impala, Trino and so on.

We also use a small trick to avoid providing a manually crafted schema. Instead of that we specify a schema of kind
`mapping` and a single property which specifies the name of the mapping whose schema will be used. This means that
Flowman will inspect the given mapping (`measurements_extracted` in this case) and infer the column names and data
types from the mapping and use that as a schema. Of course, we should reference the mapping that will also be used in
the build target as the source for writing to this relation.

```yaml
relations:
  measurements:
    kind: file
    format: parquet
    location: "$basedir/measurements/"
    # Do NOT define the pattern to be used for partitions. Then Flowman will use a standard pattern, which is
    # well understood by Spark, Hive and many other tools
    # pattern: "${year}"
    # Define data partitions. Each year is stored in a separate subdirectory
    partitions:
      - name: year
        type: integer
        granularity: 1
    # We use the inferred schema of the mapping that is written into the relation
    schema:
      kind: mapping
      mapping: measurements_extracted
```

### 2.3 Mappings
In order to read the raw data and to extract the measurements we need two different mappings. These are defined in the
file `mapping/measurements.yml`.

#### Reading Source Data
The first step is to read in the raw data. We need to specify the logical partition to read. Remember that each partition
corresponds to one year of measurement data.

```yaml
mappings:
  # This mapping refers to the "raw" relation and reads in data from the source in S3
  measurements_raw:
    kind: relation
    relation: measurements_raw
    # Set the data partition to be read
    partitions:
      year: $year
```

#### Extracting Measurements
The second mapping uses the `select` kind to extract different attributes from the raw measurements. Since the data
is stored at fixed locations within each text record, we can simply use the SQL `SUBSTR` function to extract snippets.
We also perform appropriate data type conversions and scaling as needed.

```yaml
mappings:
  # Extract multiple columns from the raw measurements data using SQL SUBSTR functions
  measurements_extracted:
    kind: select
    input: measurements_raw
    columns:
      usaf: "SUBSTR(raw_data,5,6)"
      wban: "SUBSTR(raw_data,11,5)"
      date: "TO_DATE(SUBSTR(raw_data,16,8), 'yyyyMMdd')"
      time: "SUBSTR(raw_data,24,4)"
      report_type: "SUBSTR(raw_data,42,5)"
      wind_direction: "CAST(SUBSTR(raw_data,61,3) AS INT)"
      wind_direction_qual: "SUBSTR(raw_data,64,1)"
      wind_observation: "SUBSTR(raw_data,65,1)"
      wind_speed: "CAST(CAST(SUBSTR(raw_data,66,4) AS FLOAT)/10 AS FLOAT)"
      wind_speed_qual: "SUBSTR(raw_data,70,1)"
      air_temperature: "CAST(CAST(SUBSTR(raw_data,88,5) AS FLOAT)/10 AS FLOAT)"
      air_temperature_qual: "SUBSTR(raw_data,93,1)"
```

### 2.4 Targets

Finally, we also need to slightly adjust the build target. Since we are reading the input data in partitioned chunks
per year, we also want to write the data with partitions. We already defined a partition column in the target relation,
now we need to specify the *value* of that partition column for the write operation.

```yaml
targets:
  # Define build target for measurements
  measurements:
    # Again, the target is of type "relation"
    kind: relation
    description: "Write extracted measurements per year"
    # Read records from mapping
    mapping: measurements_extracted
    # ... and write them into the relation "measurements"
    relation: measurements
    # Specify the data partition to be written
    partition:
      year: $year
```

### 2.5 Jobs
We also need to provide a job definition in `job/main.yml`, which simply references the single target defined above:

```yaml
jobs:
  # Define the 'main' job, which implicitly is used whenever you build the whole project
  main:
    # List all targets which should be built as part of the `main` job
    targets:
      - measurements
```

## 3. Execution

With all pieces in place, we can simply execute the whole project with `flowexec`:

```shell
cd /home/flowman
flowxec -f lessons/03-transformations job build main --force
```

When we want to process a different year, we can simply override the environment variable `year` on the command line
as follows:
```shell
cd /home/flowman
flowxec -f lessons/03-transformations job build main --force -D year=2014
```


## 4. Next Lessons

In the next lesson we will learn how to parametrize Flowman jobs to better fit processing of partitioned data.
