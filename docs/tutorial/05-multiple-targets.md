# Lesson 5 - Multiple Build Targets

Many projects require that multiple related data sinks need to be updated with new data. In Flowman, this would
correspond to having multiple build targets.

## 1. What to Expect

### Objectives

* You will learn how to create multiple build targets
* You will understand how Flowman builds all targets in the correct order
* You will learn how to build only individual targets via CLI
* You will gather experience with more transformations like JOINs

You can find the full source code of this lesson [on GitHub](https://github.com/dimajix/flowman-tutorial/tree/develop/lessons/05-multiple-targets)

### Description
Again, we will read in the raw measurement data, which contains many weather measurements per weather stations
(typically one measurement per weather station and per hour, but there might be more or less). This time we will
also read in the weather stations reference data and then join both data sources. Finally, we will perform a simple
aggregation on the joined data to calculate the minimum and maximum wind speed and air temperature per country and
per year.

### Processing Steps
In order to demonstrate using multiple targets, we will store some intermediate results on disk. We will define the
following build targets

#### 1. Weather Measurements
The first build target will read in the raw weather measurements from S3, extract some attributes via SQL and then
store the results as Parquet files within the local file system.

#### 2. Weather Stations
The second build target will read the weather stations reference data as CSV files from S3 and then store the data
again as Parquet files within the local file system.

#### 3. Aggregates
The third build target will read in the results from the first two build targets as Parquet files and then join
both data sets. This step enriches each weather measurement with the reference data of the corresponding weather
station. Then, we will perform an aggregation on the joined data set to calculate the minimum and maximum wind speed
and air temperature per country and per year. This result will be stored as an additional Parquet file on the local
file system.


## 2. Implementation

The implementation will pick up some code of the previous lessons, in particular from the second lesson for reading
and writing the weather stations reference data and from the forth lessons for reading and writing the measurement
data.

The logical data flow will look as follows:


```eval_rst
.. mermaid::

  flowchart LR
      r_stations_raw[[relation\n stations_raw\n CSV\n S3]] -->  m_stations_raw
      m_stations_raw{{mapping\n stations_raw}} -->  r_stations
      r_measurement_raw[[relation\n measurement_raw\n raw text\n S3]] -->  m_measurement_raw
      m_measurement_raw{{mapping\n measurement_raw}} -->  m_measurement_extracted
      m_measurement_extracted{{mapping\n measurement_raw}} -->  r_measurement
      r_stations[[relation\n stations\n Parquet\n local]] --> m_stations
      r_measurement[[relation\n measurement\n Parquet\n local]] --> m_measurement
      m_stations{{mapping\n stations}} --> m_facts
      m_measurement{{mapping\n measurement}} --> m_facts
      m_facts{{mapping\n facts}} --> m_aggregates{{mapping\n aggregates}}
      m_aggregates --> r_aggregates[[relation\n aggregates\n Parquet\n local]]
```


### 2.1 Measurements

In the first part, we will implement the following subgraph of the whole data flow:

```eval_rst
.. mermaid::

  flowchart LR
      r_measurement_raw[[relation\n measurement_raw\n raw text\n S3]] -->  m_measurement_raw
      m_measurement_raw{{mapping\n measurement_raw}} -->  m_measurement_extracted
      m_measurement_extracted{{mapping\n measurement_raw}} -->  r_measurement
```

First, we create a relation to access the raw input data stored as text files in S3:
```yaml
relations:
  # Create a relation to access the raw input data
  measurements_raw:
    kind: file
    format: text
    location: "s3a://dimajix-training/data/weather/"
    # Define the pattern to be used for partitions
    pattern: "${year}"
    # Define data partitions. Each year is stored in a separate sub directory
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
We use the same two mappings from the previous lessons for reading and extracting the relevant measurement attributes:
```yaml
mappings:
  # This mapping refers to the "raw" relation and reads in data from the source in S3
  measurements_raw:
    kind: relation
    relation: measurements_raw
    partitions:
      year: $year

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
We create a second relation which is used for the output data stored as Parquet files on the local file system:
```yaml
relations:
  # Create a second relation for storing the output data  
  measurements:
    kind: file
    format: parquet
    location: "$basedir/measurements/"
    # Define data partitions. Each year is stored in a separate sub directory
    partitions:
      - name: year
        type: integer
        granularity: 1
    # Use the inferred schema of the mapping that is written into the relation
    schema:
      kind: mapping
      mapping: measurements_extracted
```
Now we connect the output from the mapping `measurements_extracted` with the relation `measurements`:
```yaml
targets:
  # Define build target for measurements
  measurements:
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

### 2.2 Weather Stations

The weather stations reference data is processed with a similar pattern:
1. We define a relation for reading the raw CSV data from S3
2. We define a mapping to perform the actual read operation
3. We define a relation for writing the reference data to the local file system as Parquet files
4. We define a target which writes the output from the mapping into the Parquet relation.


```eval_rst
.. mermaid::

  flowchart LR
      r_stations_raw[[relation\n stations_raw\n CSV\n S3]] -->  m_stations_raw
      m_stations_raw{{mapping\n stations_raw}} -->  r_stations
```

The corresponding flow definitions look as follows:

```yaml
relations:
  # The following relation refers to the source data
  stations_raw:
    kind: file
    format: csv
    location: "s3a://dimajix-training/data/weather/isd-history/"
    options:
      sep: ","
      encoding: "UTF-8"
      quote: "\""
      header: "true"
      dateFormat: "yyyyMMdd"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"

  # The following relation refers to the reference data stored as Parquet files
  stations:
    kind: file
    description: "The 'stations' table contains meta data on all weather stations"
    format: parquet
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"

mappings:
  # This mapping refers to the "raw" relation and reads in data from the source in S3
  stations_raw:
    kind: relation
    relation: stations_raw

targets:
  # Define a build target "stations"...
  stations:
    kind: relation
    mapping: stations_raw
    relation: stations
```


### 2.3 Aggregates

In the last step, we join both data sources (measurements & reference data) and perform a simple aggregation. In order
to do so, we read from the intermediate Parquet files instead from the raw data.

```eval_rst
.. mermaid::

  flowchart LR
      r_stations[[relation\n stations\n Parquet\n local]] --> m_stations
      r_measurement[[relation\n measurement\n Parquet\n local]] --> m_measurement
      m_stations{{mapping\n stations}} --> m_joined
      m_measurement{{mapping\n measurement}} --> m_joined
      m_joined{{mapping\n measurements_joined}} --> m_facts
      m_facts{{mapping\n facts}} --> m_aggregates
      m_aggregates{{mapping\n aggregates}} --> r_aggregates[[relation\n aggregates\n Parquet\n local]]
```

This flow is implemented by the following definitions:
```yaml
mappings:
  # This mapping refers to the processed data stored as Parquet on the local filesystem
  measurements:
    kind: relation
    relation: measurements
    partitions:
      year: $year

  # This mapping refers to the Parquet relation and reads in data from the local file system
  stations:
    kind: relation
    relation: stations

  # The `measurements-joined` mapping will add station metadata to measurements
  measurements_joined:
    # Join together measurements and stations
    kind: join
    mode: left
    # Specify list of input mappings to be joined
    inputs:
      - measurements
      - stations
    # Specify columns to use for joining. Both input mappings need to contain both columns, merging is performed
    # whenever the values of both columns match in both input mappings
    columns:
      - usaf
      - wban

  # Replace invalid values with NULLs
  facts:
    kind: extend
    input: measurements_joined
    # Replace existing columns with new values, which will contain NULL values whenever the quality flags
    # indicate so
    columns:
      wind_direction: "CASE WHEN wind_direction_qual=1 THEN wind_direction END"
      wind_speed: "CASE WHEN wind_speed_qual=1 THEN wind_speed END"
      air_temperature: "CASE WHEN air_temperature_qual=1 THEN air_temperature END"

  # Create some aggregates containing min/max/avg metrics of wind speed and temperature
  aggregates:
    kind: aggregate
    input: facts
    dimensions:
      - country
    aggregations:
      min_wind_speed: "MIN(wind_speed)"
      max_wind_speed: "MAX(wind_speed)"
      avg_wind_speed: "AVG(wind_speed)"
      min_temperature: "MIN(air_temperature)"
      max_temperature: "MAX(air_temperature)"
      avg_temperature: "AVG(air_temperature)"

targets:
  # Define a target for writing the mapping "aggregates" into the relation "aggregates"
  aggregates:
    kind: relation
    description: "Write aggregated measurements per year"
    mapping: aggregates
    relation: aggregates
    partition:
      year: $year
```

### 2.4 Putting all together

Now we have defined a whole data flow including three targets. Now we simply need to create a job, which will include
all these three targets:
```yaml
jobs:
  # Define the 'main' job, which implicitly is used whenever you build the whole project
  main:
    # Add a parameter for selecting the year to process. This will create an environment variable `$year` which
    # can be accessed from within other entities like mappings, relations, etc
    parameters:
      - name: year
        type: Integer
        default: 2013
    # List all targets which should be built as part of the `main` job
    targets:
      - measurements
      - stations
      - aggregates
```
The main difference to the job definitions of the previous lessons is that the `targets` list contains multiple targets
instead of a single one.

Note that the ordering in the list of targets does not matter, although there is a clear dependency between the
target `aggregates`, which needs to be executed after the targets `stations` and `measurements`. But you do not need
to take care of these dependencies explicitly yourself, instead Flowman will analyze all the inputs and outputs of
each target and execute the targets in a correct order.

## 3. Execution

With these entities in place we ca now execute the project as follows:

```shell
cd /opt/flowman
flowexec -f lessons/05-multiple-targets job build main year=2014 --force
```
