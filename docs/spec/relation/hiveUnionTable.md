# Hive Union Table

The `hiveUnionTable` is a compound target for storing data in Hive that also provides extended schema migration
capabilities. In addition to schema changes which are supported out of the box via Hive, this target also supports
more changes like dropping columns, changing data types. This is accomplished by creating a UNION view on top of
possibly multiple Hive tables (each of them having a different incompatible schema).

## Example

```yaml
relations:
  some_table:
    kind: hiveUnionTable
    # Specify the Hive database, where the UNION view will be created
    viewDatabase: "crm"
    # Specify the name of the Hive UNION view
    view: "my_table"
    # Specify the Hive database where the underlying tables are to be created
    tableDatabase: "crm"
    # Specify the prefix of all Hive tables. Flowman will add numbers like 1,2,3,... to the prefix for 
    # different schema versions
    tablePrefix: "zz_my_table"
    # Specify the location prefix of all Hive tables. Flowman will add numbers like 1,2,3,... to the prefix for 
    # different schema versions
    locationPrefix: "/hive/crm/zz_my_table"
    external: true
    # Select file format
    format: parquet
    # Add partition column
    partitions:
    - name: landing_date
      type: string
      description: "The date on which the contract event was generated"
    # Explicitly specify the schema, which is mandatory for this relation type. In this case the schema is inferred
    # from a mapping called `some_mapping`
    schema:
      kind: mapping
      mapping: some_mapping
```

## Fields

## Fields
* `kind` **(mandatory)** *(string)*: `hiveUnionTable`

* `viewDatabase` **(optional)** *(string)* *(default: empty)*: 

* `view` **(mandatory)** *(string)*: 
Name of the view to be created and managed by Flowman.

* `viewDatabase` **(optional)** *(string)* *(default: empty)*: 
Name of the Hive database where the view should be created in

* `tablePrefix` **(mandatory)** *(string)*: 
Prefix of all tables which will be created and managed by Flowman. A version number will be appended to the prefix
to form the full table name.

* `tableDatabase` **(optional)** *(string)* *(default: empty)*: 
Name of the Hive database where the tables should be created in

 * `locationPrefix` **(optional)** *(string)* *(default: empty)*:
 Specifies the location prefix of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `LOCATION` in a `CREATE TABLE` statement.
 
 * `format` **(optional)** *(string)* *(default: empty)*:
 Specifies the format of the files stored in this Hive table. This setting is only used
 when Flowman is used to create the Hive table and is ignored otherwise. This corresponds
 to the `FORMAT` in a `CREATE TABLE` statement.

* `options` **(optional)** *(map:string)* *(default: empty)*:
  All key-value pairs specified in *options* are directly passed to Apache spark for reading
  and/or writing to this relation.

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


## Automatic Migrations

The core idea of the `hiveUnionTable` relation type is to support a broad range of non-destructive migrations. The
following changes to a data schema are supported
* New columns can be added. This will be performed without created a new underlying table.
* Existing columns can be dropped, although the previously existing columns will be still included in the Hive view
  on top of all physical tables.
* Column nullability can be changed. If changing from nullable to non-nullable, the currently underlying table will
  be reused (since the new type is stricter than before), otherwise a new table will be created.
* The data type of on existing column can be changed. Depending on the change (i.e. more or less restrictive data type), 
  either the existing physical table will be reused, or a new underlying table will be created and the view will be
  adjusted.


## Description

When using Hive union tables as data sinks in a [`relation` target](../target/relation.md), then Flowman will  manage the
whole lifecycle for you. This means that
* Hive tables will be created and migrated during `create` phase
* Hive tables will be populated with records and partitions will be added during `build` phase
* Hive tables will be truncated or individual partitions will be dropped during `clean` phase
* Hive tables will be removed during `destroy` phase

