# Fields, Data Types & Values

In various places, Flowman makes use of data type definitions. These are used for describing the layout of
data sources and sinks like CSV files but they are also used for describing external tables like Hive 

## Specifying Fields
```
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
```
name: insert_date
type: date
granularity: P1D
description: "This is the date of insertion"
```

## Specifying Values

In addition to specifying the type of some data, Flowman also requires the specification of values at some places. For
example when reading in data from a partitioned source (for example a nested directory structure or a Hive table), 
Flowman needs to now which partition(s) to read. This is also done by specifying values for the types defines above.

### Single Values
```
variable: value
```

### Array Values
```
variable: 
 - value_1
 - value_2
```


### Range Values
```
variable:
  start: 1 
  end: 10
  step: 3 
```
