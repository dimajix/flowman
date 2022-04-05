# Relations

Physical data sources (like Hive tables, relational SQL databases, files etc) are specified
using so called *relations*. Data flows can read and write from and to relations via 
appropriate mappings ([Read Mapping](../mapping/relation.md)) or build targets ([Relation
Target](../target/relation.md)).

A relation always contains all required information to connect to the data source (for 
example the table name and a [JDBC Connection](../connection/jdbc.md) in case of a JDBC
relation).

Some relations (like files, Hive tables and views) can also be directly created from Flowman,
thereby providing a single tool for schema management and data ingestion into tables. 


## Schema Conversion

Most relations support implicit schema conversions, which means that
1. when reading from a relation with a user specified schema, the actual schema of the physical data source
 is converted to the desired user defined schema
2. when writing to a relation, the schema of the data written is automatically converted to the actual schema of the
 data sink

The details of these conversions can be controlled via some config variables
* `flowman.default.relation.input.columnMismatchPolicy` (default is `IGNORE`)
* `flowman.default.relation.input.typeMismatchPolicy` (default is `IGNORE`)
* `flowman.default.relation.output.columnMismatchPolicy` (default is `ADD_REMOVE_COLUMNS`)
* `flowman.default.relation.output.typeMismatchPolicy` (default is `CAST_ALWAYS`)

The schema conversion is implemented using two aspects. The first is a mismatch between column (names). This can be
configured using the `columnMismatchPolicy` as follows. Basically the idea is that
* `IGNORE` will simply pass through the input columns unchanged
* `ERROR` will fail the build once a mismatch between actual and requested schema is detected
* `ADD_COLUMNS_OR_IGNORE` will add (`NULL`) columns from the requested schema to the input schema, and will keep columns in the input schema which are not present in the requested schema
* `ADD_COLUMNS_OR_ERROR` will add (`NULL`) columns from the requested schema to the input schema, but will fail the build if the input schema contains columns not present in the requested schema
* `REMOVE_COLUMNS_OR_IGNORE` will remove columns from the input schema which are not present in the requested schema
* `REMOVE_COLUMNS_OR_ERROR` will remove columns from the input schema which are not present in the requested schema and will fail if the input schema is missing requested columns
* `ADD_REMOVE_COLUMNS` will essentially pass through the requested schema as is

| Column Mismatch Policy     | Input Column | Requested Column | Result      |
|----------------------------|--------------|------------------|-------------|
| `IGNORE`                   | present      | missing          | present     |
| `IGNORE`                   | missing      | present          | missing     |
| `ERROR`                    | present      | missing          | build error |
| `ERROR`                    | missing      | present          | build error |
| `ADD_COLUMNS_OR_IGNORE`    | present      | missing          | present     |
| `ADD_COLUMNS_OR_IGNORE`    | missing      | present          | present     |
| `ADD_COLUMNS_OR_ERROR`     | present      | missing          | build error |
| `ADD_COLUMNS_OR_ERROR`     | missing      | present          | missing     |
| `REMOVE_COLUMNS_OR_IGNORE` | present      | missing          | missing     |
| `REMOVE_COLUMNS_OR_IGNORE` | missing      | present          | missing     |
| `REMOVE_COLUMNS_OR_ERROR`  | present      | missing          | missing     |
| `REMOVE_COLUMNS_OR_ERROR`  | missing      | present          | build error |
| `ADD_REMOVE_COLUMNS`       | present      | missing          | missing     |
| `ADD_REMOVE_COLUMNS`       | missing      | present          | present     |

The second aspect is the conversion of data types. Again this can be configured using the `typeMismatchPolicy` as
follows:

| Type Mismatch Policy        | Input vs Requested Data Type         | Result           |
|-----------------------------|--------------------------------------|------------------|
| `IGNORE`                    | Source can be cast to dest           | Input Data Type  |
| `IGNORE`                    | Source cannot be safely cast to dest | Input Data Type  |
| `ERROR`                     | Source can be cast to dest           | build error      |
| `ERROR`                     | Source cannot be safely cast to dest | build error      |
| `CAST_COMPATIBLE_OR_ERROR`  | Source can be cast to dest           | Target Data Type |
| `CAST_COMPATIBLE_OR_ERROR`  | Source cannot be safely cast to dest | build error      |
| `CAST_COMPATIBLE_OR_IGNORE` | Source can be safely cast to dest    | Target Data Type |
| `CAST_COMPATIBLE_OR_IGNORE` | Source cannot be safely cast to dest | Input Data Type  |
| `CAST_ALWAYS`               | Source can be safely cast to dest    | Target Data Type |
| `CAST_ALWAYS`               | Source cannot be safely cast to dest | Target Data Type |

## Relation Types

Flowman directly provides support for the most important data sources, which are also 
supported directly by Spark. Additional data sources can be implemented as plugins if
required.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
