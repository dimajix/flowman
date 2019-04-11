/*
 * Copyright 2018-2019 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.model

import java.net.URI
import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


object HiveTableRelation {
    val AVRO_SCHEMA_URL = "avro.schema.url"
}


class HiveTableRelation extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    @JsonProperty(value = "database", required = false) private var _database: String = ""
    @JsonProperty(value = "table", required = true) private var _table: String = ""
    @JsonProperty(value = "external", required = false) private var _external: String = "false"
    @JsonProperty(value = "location", required = false) private var _location: String = ""
    @JsonProperty(value = "format", required = false) private var _format: String = _
    @JsonProperty(value = "rowFormat", required = false) private var _rowFormat: String = _
    @JsonProperty(value = "inputFormat", required = false) private var _inputFormat: String = _
    @JsonProperty(value = "outputFormat", required = false) private var _outputFormat: String = _
    @JsonProperty(value = "properties", required = false) private var _properties: Map[String, String] = Map()
    @JsonProperty(value = "writer", required = false) private var _writer: String = "hive"

    def database(implicit context: Context): String = context.evaluate(_database)
    def table(implicit context: Context): String = context.evaluate(_table)
    def external(implicit context: Context): Boolean = context.evaluate(_external).toBoolean
    def location(implicit context: Context): Path = if (_location != null && _location.nonEmpty) new Path(context.evaluate(_location)) else null
    def format(implicit context: Context): String = context.evaluate(_format)
    def rowFormat(implicit context: Context): String = context.evaluate(_rowFormat)
    def inputFormat(implicit context: Context): String = context.evaluate(_inputFormat)
    def outputFormat(implicit context: Context): String = context.evaluate(_outputFormat)
    def properties(implicit context: Context): Map[String, String] = _properties.mapValues(context.evaluate)
    def writer(implicit context: Context): String = context.evaluate(_writer).toLowerCase(Locale.ROOT)
    def tableIdentifier(implicit context: Context): TableIdentifier = new TableIdentifier(table, Option(database))

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: StructType, partitions: Map[String, FieldValue] = Map()): DataFrame = {
        require(executor != null)
        require(partitions != null)

        implicit val context = executor.context
        logger.info(s"Reading from Hive table $tableIdentifier using partition values $partitions")

        val reader = executor.spark.read.options(options)
        val tableDf = reader.table(tableIdentifier.unquotedString)
        val df = filterPartition(tableDf, partitions)

        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: String): Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

        implicit val context = executor.context
        val schema = PartitionSchema(partitions)
        val partitionSpec = schema.spec(partition)

        if (writer == "hive")
            writeHive(executor, df, partitionSpec, mode)
        else if (writer == "spark")
            writeSpark(executor, df, partitionSpec, mode)
        else
            throw new IllegalArgumentException("Hive relations only support write modes 'hive' and 'spark'")
    }

    /**
      * Writes to a Hive table using Hive. This is the normal mode.
      *
      * @param executor
      * @param df
      * @param partitionSpec
      * @param mode
      */
    private def writeHive(executor: Executor, df: DataFrame, partitionSpec: PartitionSpec, mode: String): Unit = {
        require(executor != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        implicit val context = executor.context
        val partitionNames = partitions.map(_.name)
        logger.info(s"Writing to Hive table $tableIdentifier with partitions ${partitionNames.mkString(",")} using Hive insert")

        // Apply output schema before writing to Hive
        val outputDf = applyOutputSchema(df)

        if (partitionSpec.nonEmpty) {
            val spark = executor.spark

            val overwrite = mode.toLowerCase(Locale.ROOT) == "overwrite"
            val cmd = InsertIntoTable(
                table = UnresolvedRelation(TableIdentifier(table, Option(database))),
                partition = partitionSpec.toMap.mapValues(v => Some(v.toString)),
                query = outputDf.queryExecution.logical,
                overwrite = overwrite,
                ifPartitionNotExists = false)
            val qe = spark.sessionState.executePlan(cmd)
            SQLExecution.withNewExecutionId(spark, qe)(qe.toRdd)

            /*
            // Create temp view
            val tempViewName = "flowman_tmp_" + System.currentTimeMillis()
            outputDf.createOrReplaceTempView(tempViewName)

            // Insert data via SQL
            val writeMode = if (mode.toLowerCase(Locale.ROOT) == "overwrite") "OVERWRITE" else "INTO"
            val sql = s"INSERT $writeMode TABLE $tableIdentifier ${partitionSpec(partition)} FROM $tempViewName"
            logger.info("Inserting records via SQL: " + sql)
            spark.sql(sql).collect()

            // Remove temp view again
            spark.sessionState.catalog.dropTempView(tempViewName)
            */

            // Finally add Hive partition
            val catalog = executor.catalog
            val location = catalog.getPartitionLocation(tableIdentifier, partitionSpec)
            catalog.addOrReplacePartition(tableIdentifier, partitionSpec, location)
        }
        else {
            outputDf.write
                .mode(mode)
                .options(options)
                .insertInto(tableIdentifier.unquotedString)
        }
    }

    /**
      * Writes to Hive table by directly writing into the corresponding directory. This is a fallback and will not
      * use the Hive classes for writing.
      *
      * @param executor
      * @param df
      * @param partitionSpec
      * @param mode
      */
    private def writeSpark(executor: Executor, df: DataFrame, partitionSpec: PartitionSpec, mode: String): Unit = {
        require(executor != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        implicit val context = executor.context
        logger.info(s"Writing to Hive table $tableIdentifier with partition values $partitionSpec using direct mode")

        val location = this.location
        if (location == null)
            throw new IllegalArgumentException("Hive table relation requires 'location' for direct write mode")

        val outputPath = partitionSpec.path(location, partitions.map(_.name))

        // Perform Hive => Spark format mapping
        val format = this.format.toLowerCase(Locale.ROOT) match {
            case "avro" => "com.databricks.spark.avro"
            case _ => this.format
        }

        logger.info(s"Writing to output location '$outputPath' (partition=${partitionSpec.toMap}) as '$format'")
        this.writer(executor, df)
            .format(format)
            .mode(mode)
            .save(outputPath.toString)

        // Finally add Hive partition
        if (partitionSpec.nonEmpty) {
            val catalog = executor.catalog
            catalog.addOrReplacePartition(tableIdentifier, partitionSpec, outputPath)
        }
    }

    /**
      * Cleans either individual partitions (for partitioned tables) or truncates a whole table
      *
      * @param executor
      * @param partitions
      */
    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        implicit val context = executor.context
        logger.info(s"Cleaning Hive relation '$name' with table $tableIdentifier")

        val catalog = executor.catalog
        if (partitions.nonEmpty) {
            val partitionSchema = PartitionSchema(this.partitions)
            partitionSchema.interpolate(partitions).foreach(spec =>
                catalog.dropPartition(tableIdentifier, spec)
            )
        }
        else {
            catalog.truncateTable(tableIdentifier)
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Boolean = {
        require(executor != null)

        implicit val context = executor.context
        val catalog = executor.catalog
        catalog.tableExists(tableIdentifier)
    }

    /**
      * Creates a Hive table by executing the appropriate DDL
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists:Boolean=false): Unit = {
        require(executor != null)

        implicit val context = executor.context
        val spark = executor.spark
        val properties = this.properties
        val fields = this.fields
        val tableIdentifier = this.tableIdentifier
        val format = this.format
        logger.info(s"Creating Hive relation '$name' with table $tableIdentifier")

        // Create and save Avro schema
        import HiveTableRelation._
        if (properties.contains(AVRO_SCHEMA_URL)) {
            val avroSchemaUrl = properties(AVRO_SCHEMA_URL)
            logger.info(s"Storing Avro schema at location $avroSchemaUrl")
            new SchemaWriter(fields)
                .format("avro")
                .save(executor.context.fs.file(avroSchemaUrl))
        }

        val defaultStorage = HiveSerDe.getDefaultStorage(spark.sessionState.conf)
        val inputFormat = Option(this.inputFormat).filter(_.nonEmpty).orElse(defaultStorage.inputFormat)
        val outputFormat = Option(this.outputFormat).filter(_.nonEmpty).orElse(defaultStorage.outputFormat)
        val fileStorage = if (format != null && format.nonEmpty) {
            HiveSerDe.sourceToSerDe(format) match {
                case Some(s) =>
                    CatalogStorageFormat.empty.copy(
                        inputFormat = s.inputFormat,
                        outputFormat = s.outputFormat,
                        serde = s.serde)
                case None =>
                    throw new IllegalArgumentException(s"File format '$format' not supported")
            }
        }
        else {
            CatalogStorageFormat.empty
        }

        val catalogTable = CatalogTable(
            identifier = tableIdentifier,
            tableType = if (external) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
            storage = CatalogStorageFormat(
                locationUri = Option(location).map(_.toUri),
                inputFormat = fileStorage.inputFormat.orElse(inputFormat),
                outputFormat = fileStorage.outputFormat.orElse(outputFormat),
                serde = fileStorage.serde.orElse(Option(rowFormat)),
                compressed = false,
                properties = fileStorage.properties
            ),
            provider = Some("hive"),
            schema = StructType(fields.map(_.sparkField) ++ partitions.map(_.sparkField)),
            partitionColumnNames = partitions.map(_.name),
            properties = properties,
            comment = Option(description)
        )

        val catalog = executor.catalog
        catalog.createTable(catalogTable, ifNotExists)
/*
        val external = if (this.external) "EXTERNAL" else ""
        val create = s"CREATE $external TABLE $tableIdentifier"
        val columns = "(\n" + fields.map(field => "    " + field.name + " " + field.ftype.sqlType).mkString(",\n") + "\n)"
        val comment = Option(this.description).map(d => s"\nCOMMENT '$d')").getOrElse("")
        val partitionBy = Option(partitions).filter(_.nonEmpty).map(p => s"\nPARTITIONED BY (${p.map(p => p.name + " " + p.ftype.sqlType).mkString(", ")})").getOrElse("")
        val rowFormat = Option(this.rowFormat).map(f => s"\nROW FORMAT SERDE '$f'").getOrElse("")
        val storedAs = Option(format).map(f => s"\nSTORED AS $f").getOrElse(
            Option(inputFormat).map(f => s"\nSTORED AS INPUTFORMAT '$f'" + Option(outputFormat).map(f => s"\nOUTPUTFORMAT '$f'").getOrElse("")).getOrElse("")
        )
        val location = Option(this.location).map(l => s"\nLOCATION '$l'").getOrElse("")
        val props = if (_properties.nonEmpty) "\nTBLPROPERTIES(" + properties.map(kv => "\n    \"" + kv._1 + "\"=\"" + kv._2 + "\"").mkString(",") + "\n)" else ""
        val stmt = create + columns + comment + partitionBy + rowFormat + storedAs + location + props
        logger.info(s"Executing SQL statement:\n$stmt")
        executor.spark.sql(stmt)
*/
    }

    /**
      * Destroys the Hive table by executing an appropriate DROP statement
      *
      * @param executor
      */
    override def destroy(executor: Executor, ifExists:Boolean): Unit = {
        require(executor != null)

        implicit val context = executor.context
        logger.info(s"Destroying Hive relation '$name' with table $tableIdentifier")

        val catalog = executor.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            catalog.dropTable(tableIdentifier)
        }
    }

    override def migrate(executor: Executor): Unit = ???

    /**
      * Applies the specified schema and converts all field names to lowercase. This is required when directly
      * writing into HDFS and using Hive, since Hive only supports lower-case field names.
      *
      * @param df
      * @return
      */
    override protected def applyOutputSchema(df: DataFrame)(implicit context: Context): DataFrame = {
        val outputColumns = schema.fields.map(field => df(field.name))
        val mixedCaseDf = df.select(outputColumns: _*)
        if (needsLowerCaseSchema) {
            val lowerCaseSchema = SchemaUtils.toLowerCase(mixedCaseDf.schema)
            df.sparkSession.createDataFrame(mixedCaseDf.rdd, lowerCaseSchema)
        }
        else {
            mixedCaseDf
        }
    }

    private def needsLowerCaseSchema(implicit context: Context): Boolean = {
        false
    }

    /**
      * Creates a SQL PARTITION expression
      * @param partition
      * @return
      */
    private def partitionSpec(partition: Map[String, SingleValue])(implicit context: Context): String = {
        val schema = PartitionSchema(partitions)
        val spec = schema.spec(partition)
        HiveDialect.expr.partition(spec)
    }
}
