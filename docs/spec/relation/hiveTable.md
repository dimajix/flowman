# Hive Table Relations

The `hiveTable` relation is used for managing Hive tables.

## Examples

### Parquet Example
```yaml
relations:
  parquet_relation:
    kind: hiveTable
    database: default
    table: financial_transactions
    # Specify the physical location where the data files should be stored at. If you leave this out, the Hive
    # default location will be used
    location: /warehouse/default/financial_transactions
    # Specify the file format to use
    format: parquet
    # Add partition column
    partitions:
        - name: business_date
          type: string
    # Specify a schema, which is mandatory for write operations
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: double
```

### CSV Example
```yaml
relations:
  csv_relation:
    kind: hiveTable
    database: default
    table: financial_transactions
    # Chose `textfile` file format
    format: textfile
    # Also specify a RowFormat via a Hive class
    rowFormat: org.apache.hadoop.hive.serde2.OpenCSVSerde
    # Specify additional serialization/deserialization properties
    serdeProperties:
      separatorChar: "\t"
    # Specify a schema, which is mandatory for write operations
    schema:
      kind: inline
      fields:
        - name: id
          type: string
        - name: amount
          type: string
```

## Fields
 * `kind` **(mandatory)** *(string)*: `hiveTable`
 
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


## Remarks

When using Hive tables as data sinks in a [`relation` target](../target/relation.md), then Flowman will  manage the
whole lifecycle for you. This means that
* Hive tables will be created and migrated during `create` phase
* Hive tables will be populated with records and partitions will be added during `build` phase
* Hive tables will be truncated or individual partitions will be dropped during `clean` phase
* Hive tables will be removed during `destroy` phase

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.
