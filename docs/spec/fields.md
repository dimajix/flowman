# Fields, Data Types & Values

In various places, Flowman makes use of data type definitions. These are used for describing the layout of
data sources and sinks like CSV files, but they are also used for describing external tables like Hive

## Supported Data Types

The following simple data types are supported by Flowman

| name                | description                                                                                                                                                                                                                   |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `STRING`, `TEXT`    | text and strings of arbitrary length                                                                                                                                                                                          |
| `BINARY`            | binary data of arbitrary length                                                                                                                                                                                               |
| `TINYINT`, `BYTE`   | 8-bit signed numbers                                                                                                                                                                                                          |
| `SMALLINT`, `SHORT` | 16-bit signed numbers                                                                                                                                                                                                         |
| `INT`, `INTEGER`    | 32-bit signed numbers                                                                                                                                                                                                         |
| `BIGINT`, `LONG`    | 64-bit signed numbers                                                                                                                                                                                                         |
| `BOOLEAN`, `BOOL`   | true or false                                                                                                                                                                                                                 |
| `FLOAT`             | 32-bit floating point number                                                                                                                                                                                                  |
| `DOUBLE`            | 64-bit floating point number                                                                                                                                                                                                  |
| `DECIMAL(A,B)`      |                                                                                                                                                                                                                               |
| `VARCHAR(N)`        | text with up to `n` characters. Note that this data type is only supported for specifying input or output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length.   |
| `CHAR(N)`           | text with exactly `n` characters. Note that this data type is only supported for specifying input or output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length. |
| `DATE`              | date type                                                                                                                                                                                                                     |
| `TIMESTAMP`         | timestamp type (date and time)                                                                                                                                                                                                |
| `DURATION`          | duration type                                                                                                                                                                                                                 |

Note that Spark itself does not fully support `VARCHAR(n)` and `CHAR(n)`, which means that you cannot use these
data types in [`sql` mappings](mapping/sql.md) and SQL expressions. But Flowman well supports these data types for
all [schema definitions](schema/index.md) and in [`cast` mappings](mapping/cast.md). The main use case of these 
extended character types is to define data schemas for physical tables managed by Flowman (i.e. mainly for 
[`jdbcTable` relation](relation/jdbcTable.md)).


## Specifying Fields
In many cases Flowman provides the ability to specify *fields*, which are logical named placeholders with a fixed data
type. For example each source/target table is modelled as containing a list of columns, each of them being described
as a field. Each field has to have a name and a data type, but you can specify additional properties as follows:
```yaml
name: id
type: String
nullable: false
description: "This is the primary ID"
default:
size:
format:
charset:
collation:
```
Each field can have the following properties:
* `name` **(mandatory)** *(type: string)*: specifies the name of the column
* `type` **(mandatory)** *(type: data type)*: specifies the data type of the column
* `nullable` **(optional)** *(type: boolean)* *(default: true)*: Set to `true` if the field can contain SQL `NULL` values
* `description` **(optional)** *(type: string)*: Arbitrary user provided description, which will be used for
documentation or attached as a column comment in the target database (if supported)
* `default` **(optional)** *(type: string)* Specifies a default value
* `format` **(optional)** *(type: string)* Some relations or file formats may support different formats for example
  for storing dates
* `size` **(optional)** *(type: int)* Some relations or file formats may support different sizes of data types
* `charset` **(optional)** *(type: string)* Specifies the character set of a column. Useful for MySQL / MariaDB tables.
* `collation` **(optional)** *(type: string)* Specifies the collation of a column. Useful for SQL tables.


### Example
For example, you can specify the desired schema of a [`jdbcTable` relation](relation/jdbcTable.md) by using fields
as follows:
```yaml
relations:
  aggregates:
    kind: jdbcTable
    connection: jdbcConnection
    table: weather
    schema:
      kind: inline
      fields:
        - name: date
          type: DATE
          nullable: false
        - name: country
          type: CHAR(2)
          nullable: false
          description: "FIPS Country Code"
        - name: min_wind_speed
          type: FLOAT
          description: "Minimum wind speed, which will explain the wind speed but not the direction, which is provided in another cölumn"
        - name: max_wind_speed
          type: FLOAT
          description: "Maximum wind speed"
        - name: avg_wind_speed
          type: DOUBLE
        - name: comment
          type: VARCHAR(32)
          charset: utf8mb4
          collation: utf8mb4_general_ci
```


## Specifying Partition Columns
In addition to normal schema definitions for CSV files, Flowman also supports the definition of partition columns used
for organizing all data in different directories (like in Hive, but also raw files on HDFS or S3)
```yaml
relations:
  input_data:
    kind: files
    location: "${adcount_logdir}"
    pattern: "${insert_date.format('yyyy/MM/dd')}/*.log"
    partitions:
      - name: insert_date
        type: date
        granularity: P1D
        description: "This is the date of insertion"
```
Each partition column has the following properties:
* `name` **(mandatory)** *(type: string)*: specifies the name of the column
* `type` **(mandatory)** *(type: data type)*: specifies the data type of the column
* `description` **(optional)** *(type: string)*: Arbitrary user provided description, which will be used for
  documentation or attached as a column comment in the target database (if supported)
* `granularity` **(optional)** *(type: string)*: This field defines the *granularity* of the partition column. For
example, if a partition column contains timestamps, you may specify a granularity of "PT15M" representing 15 minutes.
This means that Flowman will assume that all partitions are truncated to 15 minutes. Providing a granularity may be
important when you refer to a range of partitions (see below).


## Specifying Values

In addition to specifying the type of some data, Flowman also requires the specification of values at some places. For
example when reading in data from a partitioned source (for example a nested directory structure or a Hive table), 
Flowman needs to now which partition(s) to read. This is also done by specifying values for the types defines above.

### Single Values
The simplest case is to specify a single value.
```yaml
mappings:
  input_data_raw:
    kind: read
    relation: "input_data"
    partitions:
      insert_date: "$start_dt"
```

### Array Values
It is also possible to specify an explicit list of values. Flowman will insert all these values one after the other
into the variable.
```yaml
mappings:
  input_data_raw:
    kind: read
    relation: "input_data"
    partitions:
      insert_date:
        - "${LocalDate.parse($start_dt)"
        - "${LocalDate.addDays($end_dt, 1)}"
```


### Range Values
```yaml
mappings:
  input_data_raw:
    kind: read
    relation: "input_data"
    partitions:
      insert_date:
        start: "${LocalDate.addDays($start_dt, -3)"
        end: "${LocalDate.addDays($end_dt, 7)}"
        step: "P1D"
```
