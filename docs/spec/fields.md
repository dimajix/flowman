# Fields, Data Types & Values

In various places, Flowman makes use of data type definitions. These are used for describing the layout of
data sources and sinks like CSV files but they are also used for describing external tables like Hive 

## Specifying Fields
```yaml
name: id
type: String
nullable: false
description: "This is the primary ID"
default:
size:
format:
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
