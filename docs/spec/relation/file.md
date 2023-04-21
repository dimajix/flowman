# File Relations

File relations are among the most simple relation types. They refer to data stored in individual files, typically on
a distributed and shared file system or object store like Hadoop HDFS or S3.

## Example
```yaml
relations:
  csv_export:
    kind: file
    # Specify the file format to use
    format: "csv"
    # Specify the base directory where all data is stored. This location does not include the partition pattern
    location: "${export_dir}"
    # You could specify the pattern how to identify files and/or partitions. This pattern is relative to the `location`.
    # Actually, it is highly recommended NOT to explicitly specify a partition pattern for outgoing relations
    # and let Spark generate this according to the Hive standard.
    pattern: "${export_pattern}"
    # Set format specific options
    options:
      delimiter: ","
      quote: "\""
      escape: "\\"
      header: "true"
      compression: "gzip"
    # Add partition column, which can be used in the `pattern`
    partitions:
      - name: datetime
        type: timestamp
        granularity: "P1D"
    # Specify an optional schema here. It is always recommended to explicitly specify a schema for every relation
    # and not just let data flow from a mapping into a target.
    schema:
      kind: inline
      fields:
        - name: country
          type: STRING
        - name: min_wind_speed
          type: FLOAT
        - name: max_wind_speed
          type: FLOAT
```

## Fields
 * `kind` **(mandatory)** *(string)*: `file`
 
 * `schema` **(optional)** *(schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema if the underlying file format supports this.

 * `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `options` **(optional)** *(map:string)* *(default: empty)*:
 Options are passed directly to Spark for reading and/or writing data. Options are specific
 to the selected file format. Best is to refer to the Apache Spark Documentation.
 
 * `format` **(optional)** *(string)* *(default: csv)*:
 This specifies the file format to use. All formats supported by Apache Spark can be used,
 for example `csv`, `parquet`, `orc`, `avro` and `json`
  
 * `location` **(mandatory)** *(string)*:
 This field specifies the storage location in the Hadoop compatible file system. If the data 
 source is partitioned, this should  specify only the root location below which partition 
 directories are created.
 
 * `partitions` **(optional)** *(list:partition)* *(default: empty)*:
 In order to use partitioned file based data sources, you need to define the partitioning columns. Each partitioning 
   column has a name and a type and optionally a granularity. Normally the partition columns are separate from the
   schema, but you *may* also include the partition column in the schema, although this is not considered to be best 
   practice. But it turns out to be quite useful in combination with dynamically writing to multiple partitions.

 * `pattern` **(optional)** *(string)* *(default: empty)*:
 This field specifies the directory and/or file name pattern to access specific partitions. 
 Please see the section [Partitioning](#Partitioning) below. 


## Automatic Migrations
The `file` relation does not support any automatic migration like adding/removing columns. 


## Schema Conversion
The `file` relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).


## Output Modes

### Batch Writing
The `file` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                                            |
|---------------------|-----------|---------------------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the files already exists                          |
| `ignoreIfExists`    | yes       | Do nothing if the files already exists                              |
| `overwrite`         | yes       | Overwrite the whole location or the specified partitions            |
| `overwrite_dynamic` | yes       | Overwrite only partitions dynamically determined by the data itself |
| `append`            | yes       | Append new records to the existing files                            |
| `update`            | no        | -                                                                   |

### Stream Writing
In addition to batch writing, the file relation also supports stream writing via the
[`stream` target](../target/stream.md) with the following semantics:

| Output Mode | Supported | Comments                                                                      |
|-------------|-----------|-------------------------------------------------------------------------------|
| `append`    | yes       | Append new records from the streaming process once they don't change any more |
| `update`    | yes       | Append records every time they are updated                                    |
| `complete`  | no        | -                                                                             |


## Remarks

When using `file` relations as data sinks in a [`relation` target](../target/relation.md), then Flowman will manage the
whole lifecycle of the directory for you. This means that
* The directory specified in `location` will be created during `create` phase
* The directory specified in `location` will be populated with records or partitioning subdirectories will be added 
  during `build` phase
* The directory specified in `location` will be truncated or individual partitions will be dropped during `clean` phase
* The directory specified in `location` tables will be removed during `destroy` phase

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.

### Partitioning

Table partitioning is a common optimization approach used in systems like Hive. In a partitioned table, data are usually
stored in different directories, with partitioning column values encoded in the path of each partition directory. All 
file formats (including Text/CSV/JSON/ORC/Parquet) are able to discover and infer partitioning information 
automatically. For example, we can store all our previously used population data into a partitioned table using the 
following directory structure, with two extra columns, gender and country as partitioning columns:
```text
path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...
```
By specifying `path/to/table` as the `location`, Flowman will automatically extract the partitioning information from 
the paths. The schema will look as follows
```text
root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)
```

Flowman also supports partitioning, i.e. written to different subdirectories. You can explicitly specify a *partition
pattern* via the `pattern` field, but it is highly recommended to NOT explicitly set this field and let Spark manage
partitions itself. This way Spark can infer partition values from directory names and will also list directories more
efficiently.

### Writing to Dynamic Partitions

Beside explicitly writing to a single Hive partition, Flowman also supports to write to multiple partitions where
the records need to contain values for the partition columns. This feature cannot be combined with explicitly specifying
a value for the file `pattern`, instead the standard Hive directory pattern for partitioned data will be used (i.e. 
`location/partition=value/`).


### Supported File Format

File relations support all file formats also supported by Spark. This includes simple text files, CSV files,
Parquet files, ORC files and Avro files. Each file format provides its own additional settings which can be specified
in the `options` section.

#### Text

The format `text` will parse text files, where each row will be interpreted as one record with one column called
`value`. You can change the columns name by explicitly specifying a schema.

The following options are supported:

| Property Name | Default                                      | Meaning                                                                                                                                                  | Scope      |
|---------------|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| wholetext     | false                                        | If true, read each file from input path(s) as a single row.                                                                                              | read       |
| lineSep       | \r, \r\n, \n (for reading), \n (for writing) | Defines the line separator that should be used for reading or writing.                                                                                   | read/write |
| compression   | (none)                                       | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).  | write      |


#### CSV

Flowman supports CSV files via the `csv` format to read a file or directory of files in CSV format. The following 
options can be used to customize the behavior of reading or writing, such as controlling behavior of the header, 
delimiter character, character set, and so on.

| Property Name             | Default                                                        | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Scope      |
|---------------------------|----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| sep                       | ,                                                              | Sets a separator for each field and value. This separator can be one or more characters.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read/write |
| encoding                  | UTF-8                                                          | For reading, decodes the CSV files by the given encoding type. For writing, specifies encoding (charset) of saved CSV files. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                  | read/write |
| quote                     | "                                                              | Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string. For writing, if an empty string is set, it uses u0000 (null character).                                                                                                                                                                                                                                                                       | read/write |
| quoteAll                  | false                                                          | A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character.                                                                                                                                                                                                                                                                                                                                                                                                                 | write      |
| escape                    | \                                                              | Sets a single character used for escaping quotes inside an already quoted value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read/write |
| escapeQuotes              | true                                                           | A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character.                                                                                                                                                                                                                                                                                                                                                                                                    | write      |
| comment                   |                                                                | Sets a single character used for skipping lines beginning with this character. By default, it is disabled.                                                                                                                                                                                                                                                                                                                                                                                                                                               | read       |
| header                    | false                                                          | For reading, uses the first line as names of columns. For writing, writes the names of columns as the first line.                                                                                                                                                                                                                                                                                                                                                                                                                                        | read/write |
| inferSchema               | false                                                          | Infers the input schema automatically from data. It requires one extra pass over the data.                                                                                                                                                                                                                                                                                                                                                                                                                                                               | read       |
| preferDate                | true                                                           | During schema inference (inferSchema), attempts to infer string columns that contain dates as Date if the values satisfy the dateFormat option or default date format. For columns that contain a mixture of dates and timestamps, try inferring them as `TIMESTAMP` if timestamp format not specified, otherwise infer them as `STRING`.                                                                                                                                                                                                                | read       |
| enforceSchema             | true                                                           | If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive. Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results. | read       |
| ignoreLeadingWhiteSpace   | false (for reading), true (for writing)                        | A flag indicating whether or not leading whitespaces from values being read/written should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                   | read/write |
| ignoreTrailingWhiteSpace  | false (for reading), true (for writing)                        | A flag indicating whether or not trailing whitespaces from values being read/written should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                  | read/write |
| nullValue                 |                                                                | Sets the string representation of a null value. This nullValue param applies to all supported types including the string type.                                                                                                                                                                                                                                                                                                                                                                                                                           | read/write |
| nanValue                  | NaN                                                            | Sets the string representation of a non-number value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | read       |
| positiveInf               | Inf                                                            | Sets the string representation of a positive infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | read       |
| negativeInf               | -Inf                                                           | Sets the string representation of a negative infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | read       |
| dateFormat                | `yyyy-MM-dd`                                                   | Sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type.                                                                                                                                                                                                                                                                                                                                                                                                                    | read/write |
| timestampFormat           | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`                             | Sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type.                                                                                                                                                                                                                                                                                                                                                                                                          | read/write |
| timestampNTZFormat        | `yyyy-MM-dd'T'HH:mm:ss[.SSS]`                                  | Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type.                                                                                                                                                                                                                                                               | read/write |
| maxColumns                | 20480                                                          | Defines a hard limit of how many columns a record can have.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | read       |
| maxCharsPerColumn         | -1                                                             | Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length                                                                                                                                                                                                                                                                                                                                                                                                                           | read       |
| mode                      | `PERMISSIVE`                                                   | Can be either `PERMISSIVE`,  `DROPMALFORMED` or `FAILFAST`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | read       |
| columnNameOfCorruptRecord | (value of `spark.sql.columnNameOfCorruptRecord` configuration) | Allows renaming the new field having malformed string created by `PERMISSIVE` mode. This overrides spark.sql.columnNameOfCorruptRecord.                                                                                                                                                                                                                                                                                                                                                                                                                  | read       |
| multiLine                 | false                                                          | Parse one record, which may span multiple lines, per file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | read       |
| charToEscapeQuoteEscaping | escape or \0                                                   | Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise.                                                                                                                                                                                                                                                                                                                                                                        | read/write |
| samplingRatio             | 1.0                                                            | Defines fraction of rows used for schema inferring.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | read       |
| emptyValue                | (for reading), "" (for writing)                                | Sets the string representation of an empty value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | read/write |
| locale                    | en-US                                                          | Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.                                                                                                                                                                                                                                                                                                                                                                                                                                      | read       |
| lineSep                   | \r, \r\n and \n (for reading), \n (for writing)                | Defines the line separator that should be used for parsing/writing.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | read/write |
| unescapedQuoteHandling    | STOP_AT_DELIMITER                                              | Defines how the CsvParser will handle values with unescaped quotes. STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
| compression               | (none)                                                         | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).                                                                                                                                                                                                                                                                                                                                                                                                  | write      |


#### JSON

Flowman can automatically infer the schema of a JSON dataset, when no schema is specified. 
Note that the file that is offered as a json file is not a typical JSON file. Each line must contain a separate, self-contained valid JSON object.

Although JSON is a very well defined standard, the following options can be set to control some details:

| Property Name                      | Default                                                                                 | Meaning                                                                                                                                                                                                                                                                                    | Scope      |
|------------------------------------|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| timeZone                           | (value of `spark.sql.session.timeZone` configuration)                                   | Sets the string that indicates a time zone ID to be used to format timestamps in the JSON datasources or partition values.                                                                                                                                                                 | read/write |  
| primitivesAsString                 | false                                                                                   | Infers all primitive values as a string type.                                                                                                                                                                                                                                              | read       |  
| prefersDecimal                     | false                                                                                   | Infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.                                                                                                                                                                   | read       |  
| allowComments                      | false                                                                                   | Ignores Java/C++ style comment in JSON records.                                                                                                                                                                                                                                            | read       |  
| allowUnquotedFieldNames            | false                                                                                   | Allows unquoted JSON field names.                                                                                                                                                                                                                                                          | read       |  
| allowSingleQuotes                  | true                                                                                    | Allows unquoted JSON field names.                                                                                                                                                                                                                                                          | read       |  
| allowNumericLeadingZeros           | false                                                                                   | Allows leading zeros in numbers (e.g. 00012).                                                                                                                                                                                                                                              | read       |  
| allowBackslashEscapingAnyCharacter | false                                                                                   | Allows accepting quoting of all character using backslash quoting mechanism.                                                                                                                                                                                                               | read       |  
| mode                               | `PERMISSIVE`                                                                            | Can be either `PERMISSIVE`,  `DROPMALFORMED` or `FAILFAST`                                                                                                                                                                                                                                 | read       |
| columnNameOfCorruptRecord          | (value of `spark.sql.columnNameOfCorruptRecord` configuration)                          | Allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.                                                                                                                                                      | read       |
| dateFormat                         | yyyy-MM-dd                                                                              | Sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type.                                                                                                                                                      | read/write |
| timestampFormat                    | yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]                                                        | Sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type.                                                                                                                                            | read/write |
| timestampNTZFormat                 | yyyy-MM-dd'T'HH:mm:ss[.SSS]                                                             | Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type. | read/write |
| multiLine                          | false                                                                                   | Parse one record, which may span multiple lines, per file.                                                                                                                                                                                                                                 | read       |  
| allowUnquotedControlChars          | false                                                                                   | Allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.                                                                                                                                      | read       |  
| encoding                           | Detected automatically when multiLine is set to true (for reading), UTF-8 (for writing) | For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. For writing, Specifies encoding (charset) of saved json files.                                                                                          | read/write |  
| lineSep                            | \r, \r\n, \n (for reading), \n (for writing)                                            | Defines the line separator that should be used for parsing.                                                                                                                                                                                                                                | read/write |  
| samplingRatio                      | 1.0                                                                                     | Defines fraction of rows used for schema inferring.                                                                                                                                                                                                                                        | read       |
| dropFieldIfAllNull                 | false                                                                                   | Whether to ignore column of all null values or empty array during schema inference.                                                                                                                                                                                                        | read       |  
| locale                             | en-US                                                                                   | Sets a locale as language tag in IETF BCP 47 format. For instance, locale is used while parsing dates and timestamps.                                                                                                                                                                      | read       |  
| allowNonNumericNumbers             | true                                                                                    | Allows JSON parser to recognize set of “Not-a-Number” (NaN) tokens as legal floating number values.                                                                                                                                                                                        | read       |  
| compression                        | (none)                                                                                  | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate). JSON built-in functions ignore this option.                                                                                        | write      |  
| ignoreNullFields                   | (value of `spark.sql.jsonGenerator.ignoreNullFields`) configuration                     | Whether to ignore null fields when generating JSON objects.                                                                                                                                                                                                                                | write      |  
|                                    |                                                                                         |                                                                                                                                                                                                                                                                                            |            |  


#### Parquet

Parquet is a columnar format that is supported by many other data processing systems. Flowman provides support for both
reading and writing Parquet files that automatically preserves the schema of the original data. When reading Parquet 
files, all columns are automatically converted to be nullable for compatibility reasons.

| Property Name      | Default                                                               | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Scope  |
|--------------------|-----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| datetimeRebaseMode | (value of `spark.sql.parquet.datetimeRebaseModeInRead` configuration) | The datetimeRebaseMode option allows to specify the rebasing mode for the values of the DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic Gregorian calendar. Can be EXCEPTION, CORRECTED or LEGACY                                                                                                                                                                                                                                  | read   |  
| int96RebaseMode    | (value of `spark.sql.parquet.int96RebaseModeInRead` configuration)    | The datetimeRebaseMode option allows to specify the rebasing mode for the values of the DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic Gregorian calendar.                                                                                                                                                                                                                                                                        | read   |
| mergeSchema        | (value of `spark.sql.parquet.mergeSchema` configuration)              | Sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.parquet.mergeSchema.                                                                                                                                                                                                                                                                                                                                          | read   |
| compression        | snappy                                                                | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd). This will override spark.sql.parquet.compression.codec.                                                                                                                                                                                                                                      | write  |
|                    |                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |        |
|                    |                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |        |
|                    |                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |        |
|                    |                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |        |
|                    |                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |        |


#### ORC
Apache ORC is a columnar format which has more advanced features like native zstd compression, bloom filter and 
columnar encryption.

Spark (and Flowman) supports two ORC implementations (native and hive) which is controlled by `spark.sql.orc.impl`. Two 
implementations share most functionalities with different design goals.

* `native` implementation is designed to follow Spark’s data source behavior like Parquet.
* `hive` implementation is designed to follow Hive’s behavior and uses Hive SerDe.

For example, historically, native implementation handles CHAR/VARCHAR with Spark’s native String while hive 
implementation handles it via Hive CHAR/VARCHAR. The query results are different. Since Spark 3.1.0, 
SPARK-33480 removes this difference by supporting CHAR/VARCHAR from Spark-side.

The following options are available for ORC files:

| Property Name      | Default                                              | Meaning                                                                                                                                                                                                      | Scope  |
|--------------------|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| mergeSchema        | (value of `spark.sql.orc.mergeSchema configuration`) | Sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.orc.mergeSchema.                                                                                    | read   |
| compression        | snappy                                               | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, snappy, zlib, lzo, lz4 and zstd). This will override `spark.sql.parquet.compression.codec`. | write  |


#### Avro

The following options are available for Avro files:

| Property Name            | Default                                                            | Meaning                                                                                                                                                                                                                                                                                                                                              | Scope      |
|--------------------------|--------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| avroSchema               |                                                                    | Optional schema provided by a user in JSON format.                                                                                                                                                                                                                                                                                                   | read/write |
| recordName               | topLevelRecord                                                     | Top level record name in write result, which is required in Avro spec.                                                                                                                                                                                                                                                                               | write      |
| recordNamespace          |                                                                    | Record namespace in write result.                                                                                                                                                                                                                                                                                                                    | write      |
| compression              | true                                                               | The compression option allows to specify a compression codec used in write. Currently supported codecs are uncompressed, snappy, deflate, bzip2, xz and zstandard. If the option is not set, the configuration `spark.sql.avro.compression.codec` config is taken into account.                                                                      | read       |
| datetimeRebaseMode       | (value of `spark.sql.avro.datetimeRebaseModeInRead` configuration) | The datetimeRebaseMode option allows to specify the rebasing mode for the values of the DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic Gregorian calendar. Can be EXCEPTION, CORRECTED or LEGACY                                                                                                                | read       |  
| positionalFieldMatching  | false                                                              | This can be used in tandem with the `avroSchema` option to adjust the behavior for matching the fields in the provided Avro schema with those in the SQL schema. By default, the matching will be performed using field names, ignoring their positions. If this option is set to "true", the matching will be based on the position of the fields.  | read/write |
