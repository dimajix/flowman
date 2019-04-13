---
layout: page
title: Flowman Hive Table Relation
permalink: /spec/relation/table.html
---
# Hive Table Relations

## Examples

### Parquet Example
```
relations:
  parquet_relation:
    kind: hiveTable
    database: default
    table: financial_transactions
    location: /warehouse/default/financial_transactions
    format: parquet
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: double
    partitions:
      - name: business_date
        type: string
```

### CSV Example
```
relations:
  csv_relation:
    kind: hiveTable
    database: default
    table: financial_transactions
    format: textfile
    rowFormat: org.apache.hadoop.hive.serde2.OpenCSVSerde
    serdeProperties:
      separatorChar: "\t"
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: string
```

## Fields
 * `kind` **(mandatory)** *(string)*: `table` or `hiveTable`
 
 * `schema` **(optional)** *(schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema.
 
 * `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `options` **(optional)** *(map:string)* *(default: empty)*:
 All key-value pairs specified in *options* are directly passed to Apache spark for reading
 and/or writing to this relation.
 
 * `database` **(mandatory)** *(string)*:
 Defines the Hive database where the table is defined. When no database is specified, the
 table is accessed without any specific qualification, meaning that the default database
 will be used.
  
 * `table` **(mandatory)** *(string)*:
 Contains the name of the Hive table.
 
 * `external` **(optional)** *(boolean)* *(default: false)*: 
 Set to *true* if the Hive table should be created as `EXTERNAL` otherwise false. This flag
 is only used when Flowman is used to create the Hive table and is ignored  otherwise.
 
 * `location` **(optional)** *(string)* *(default: empty)*:
 Specifies the location of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `LOCATION` in a `CREATE TABLE` statement.
 
 * `format` **(optional)** *(string)* *(default: empty)*:
 Specifies the format of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `FORMAT` in a `CREATE TABLE` statement.

 * `rowFormat` **(optional)** *(string)* *(default: empty)*:
 Specifies the row format of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `ROW FORMAT` in a `CREATE TABLE` statement.

 * `inputFormat` **(optional)** *(string)* *(default: empty)*:
 Specifies the input format of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `INPUT FORMAT` in a `CREATE TABLE` statement.

 * `outputFormat` **(optional)** *(string)* *(default: empty)*:
 Specifies the input format of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `OUTPUT FORMAT` in a `CREATE TABLE` statement.

 * `partitions` **(optional)** *(list:partition)* *(default: empty)*:
 Specifies all partition columns. This is used both for creating Hive tables, but also for
 writing and reading to and from them. Therefore if you are working with partitioned Hive
 tables **you have to specify partition columns, even if Flowman is not used for creating
 the table**.

 * `properties` **(optional)** *(map:string)* *(default: empty)*:
 Specifies additional properties of the Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `TBLPROPERTIES` in a `CREATE TABLE` statement.

 * `writer` **(optional)** *(string)* *(default: hive)*:
 Flowman supports two different mechanisms for writing to a Hive table. In `hive` mode
 Spark uses Hive libraries to write to the table. In `spark` mode, Flowman will use
 the `location` and write the files itself. This does not always result in the same 
 files and can be used to workaround some bugs in the Hive backend.


## Description
