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

package com.dimajix.flowman.spec.relation

import java.io.FileNotFoundException
import java.util.Locale

import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Unknown
import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.IncompatibleSchemaException
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.hadoop.FileUtils
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


object HiveTableRelation {
    val AVRO_SCHEMA_URL = "avro.schema.url"
}


case class HiveTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    override val database: Option[String] = None,
    override val table: String,
    external: Boolean = false,
    location: Option[Path] = None,
    format: Option[String] = None,
    options: Map[String,String] = Map(),
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map(),
    serdeProperties: Map[String, String] = Map(),
    writer: String = "hive"
) extends HiveRelation with SchemaRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    /**
      * Returns the list of all resources which will be created by this relation. The list will be specifically
      * created for a specific partition, or for the full relation (when the partition is empty)
      *
      * @param partition
      * @return
      */
    override def resources(partition: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partition)

        val allPartitions = PartitionSchema(this.partitions).interpolate(partition)
        allPartitions.map(p => ResourceIdentifier.ofHivePartition(table, database, p.toMap)).toSet
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofHiveTable(table, database)
    )

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires : Set[ResourceIdentifier] = {
        database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode:OutputMode = OutputMode.OVERWRITE): Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

        requireAllPartitionKeys(partition)

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
    private def writeHive(executor: Executor, df: DataFrame, partitionSpec: PartitionSpec, mode:OutputMode): Unit = {
        require(executor != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        logger.info(s"Writing Hive relation '$identifier' to table '$tableIdentifier' partition ${HiveDialect.expr.partition(partitionSpec)} with mode '$mode' using Hive insert")

        // Apply output schema before writing to Hive
        val outputDf = applyOutputSchema(executor, df)

        // Helper method for Spark < 2.4
        implicit def toAttributeNames(atts:Seq[Attribute]) : Seq[String] = atts.map(_.name)

        val spark = executor.spark
        val catalog = executor.catalog

        if (partitionSpec.nonEmpty) {
            val hiveTable = catalog.getTable(TableIdentifier(table, database))
            val query = outputDf.queryExecution.logical

            val overwrite = mode == OutputMode.OVERWRITE
            val cmd = InsertIntoHiveTable(
                table = hiveTable,
                partition = partitionSpec.toMap.mapValues(v => Some(v.toString)),
                query = query,
                overwrite = overwrite,
                ifPartitionNotExists = false,
                query.output
            )
            val qe = spark.sessionState.executePlan(cmd)
            SparkShim.withNewExecutionId(spark, qe)(qe.toRdd)

            // Finally refresh Hive partition
            catalog.refreshPartition(tableIdentifier, partitionSpec)
        }
        else {
            // Create and configure writer
            val writer = outputDf.write
                .mode(mode.batchMode)
                .options(options)
            format.foreach(writer.format)

            writer.insertInto(tableIdentifier.unquotedString)

            catalog.refreshTable(tableIdentifier)
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
    private def writeSpark(executor: Executor, df: DataFrame, partitionSpec: PartitionSpec, mode:OutputMode): Unit = {
        require(executor != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        logger.info(s"Writing Hive relation '$identifier' to table '$tableIdentifier' partition ${HiveDialect.expr.partition(partitionSpec)} with mode '$mode' using direct write")

        if (location.isEmpty)
            throw new IllegalArgumentException("Hive table relation requires 'location' for direct write mode")

        val outputPath = partitionSpec.path(location.get, partitions.map(_.name))

        // Perform Hive => Spark format mapping
        val format = this.format.map(_.toLowerCase(Locale.ROOT)) match {
            case Some("avro") => "com.databricks.spark.avro"
            case Some(f) => f
            case None => throw new IllegalArgumentException("Require 'format' for directly writing to Hive tables")
        }

        logger.info(s"Writing to output location '$outputPath' (partition=${partitionSpec.toMap}) as '$format'")
        this.writer(executor, df, format, options, mode.batchMode)
            .save(outputPath.toString)

        // Finally add Hive partition
        val catalog = executor.catalog
        if (partitionSpec.nonEmpty) {
            catalog.addOrReplacePartition(tableIdentifier, partitionSpec, outputPath)
        }
        else {
            catalog.refreshTable(tableIdentifier)
        }
    }

    /**
      * Cleans either individual partitions (for partitioned tables) or truncates a whole table
      *
      * @param executor
      * @param partitions
      */
    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val catalog = executor.catalog
        // When no partitions are specified, this implies that the whole table is to be truncated
        if (partitions.nonEmpty) {
            val partitionSchema = PartitionSchema(this.partitions)
            partitionSchema.interpolate(partitions).foreach { spec =>
                logger.info(s"Cleaning Hive relation '$identifier' by truncating table '$tableIdentifier' partition ${HiveDialect.expr.partition(spec)}")
                catalog.dropPartition(tableIdentifier, spec)
            }
        }
        else {
            logger.info(s"Cleaning Hive relation '$identifier' by truncating table '$tableIdentifier'")
            catalog.truncateTable(tableIdentifier)
        }
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param executor
     * @param partition
     * @return
     */
    override def loaded(executor: Executor, partition: Map[String, SingleValue]): Trilean = {
        require(executor != null)
        require(partition != null)

        requireValidPartitionKeys(partition)

        val catalog = executor.catalog
        if (partitions.nonEmpty) {
            val schema = PartitionSchema(partitions)
            val partitionSpec = schema.spec(partition)
            catalog.tableExists(tableIdentifier) &&
                catalog.partitionExists(tableIdentifier, partitionSpec)
        }
        else {
            // Since we do not know for an unpartitioned table if it contains data, we simply return "Unknown"
            if (catalog.tableExists(tableIdentifier)) {
                val location = catalog.getTableLocation(tableIdentifier)
                val fs = location.getFileSystem(executor.hadoopConf)
                FileUtils.isValidHiveData(fs, location)
            }
            else {
                No
            }
        }
    }

    /**
      * Creates a Hive table by executing the appropriate DDL
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists:Boolean=false): Unit = {
        require(executor != null)

        if (!ifNotExists || exists(executor) == No) {
            val sparkSchema = StructType(fields.map(_.sparkField))
            logger.info(s"Creating Hive table relation '$identifier' with table $tableIdentifier and schema\n ${sparkSchema.treeString}")

            // Create and save Avro schema
            import HiveTableRelation._
            if (properties.contains(AVRO_SCHEMA_URL)) {
                val avroSchemaUrl = properties(AVRO_SCHEMA_URL)
                logger.info(s"Storing Avro schema at location $avroSchemaUrl")
                new SchemaWriter(schema.toSeq.flatMap(_.fields))
                    .format("avro")
                    .save(executor.fs.file(avroSchemaUrl))
            }

            val defaultStorage = HiveSerDe.getDefaultStorage(executor.spark.sessionState.conf)
            val fileStorage: CatalogStorageFormat = if (format.exists(_.nonEmpty)) {
                HiveSerDe.sourceToSerDe(format.get) match {
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

            // Selection rule:
            //  1. use explicitly specified format
            //  2. use format from file storage
            //  3. use default format
            val inputFormat = this.inputFormat
                .filter(_.nonEmpty)
                .orElse(fileStorage.inputFormat)
                .orElse(defaultStorage.inputFormat)
            val outputFormat = this.outputFormat
                .filter(_.nonEmpty)
                .orElse(fileStorage.outputFormat)
                .orElse(defaultStorage.outputFormat)
            val rowFormat = this.rowFormat.filter(_.nonEmpty).orElse(fileStorage.serde).orElse(defaultStorage.serde)

            // Configure catalog table by assembling all options
            val catalogTable = CatalogTable(
                identifier = tableIdentifier,
                tableType = if (external) CatalogTableType.EXTERNAL
                else CatalogTableType.MANAGED,
                storage = CatalogStorageFormat(
                    locationUri = location.map(_.toUri),
                    inputFormat = inputFormat,
                    outputFormat = outputFormat,
                    serde = rowFormat,
                    compressed = false,
                    properties = fileStorage.properties ++ serdeProperties
                ),
                provider = Some("hive"),
                schema = sparkSchema,
                partitionColumnNames = partitions.map(_.name),
                properties = properties,
                comment = description
            )

            // Create table
            val catalog = executor.catalog
            catalog.createTable(catalogTable, false)
        }
    }

    /**
      * Destroys the Hive table by executing an appropriate DROP statement
      *
      * @param executor
      */
    override def destroy(executor: Executor, ifExists:Boolean): Unit = {
        require(executor != null)

        val catalog = executor.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            logger.info(s"Destroying Hive table relation '$identifier' by dropping table $tableIdentifier")
            catalog.dropTable(tableIdentifier)
        }
    }

    /**
      * Performs migration of a Hive table by adding new columns
      * @param executor
      */
    override def migrate(executor: Executor): Unit = {
        require(executor != null)

        val catalog = executor.catalog
        if (catalog.tableExists(tableIdentifier)) {
            val table = catalog.getTable(tableIdentifier)
            if (table.tableType == CatalogTableType.VIEW) {
                logger.warn(s"TABLE target $tableIdentifier is currently a VIEW, dropping...")
                catalog.dropTable(tableIdentifier, false)
                create(executor, false)
            }
            else {
                val sourceSchema = schema.get.sparkSchema
                val targetSchema = table.dataSchema
                val targetFields = targetSchema.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap

                // Ensure that current real Hive schema is compatible with specified schema
                val isCompatible = sourceSchema.forall { field =>
                    targetFields.get(field.name.toLowerCase(Locale.ROOT))
                        .forall(tgt => SchemaUtils.isCompatible(field, tgt))
                }
                if (!isCompatible) {
                    logger.error(s"Cannot migrate existing schema\n ${targetSchema.treeString}\n to new schema\n ${sourceSchema.treeString}")
                    throw new IncompatibleSchemaException(identifier)
                }

                val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name.toLowerCase(Locale.ROOT)))
                if (missingFields.nonEmpty) {
                    val newSchema = StructType(targetSchema.fields ++ missingFields)
                    logger.info(s"Migrating HiveTable relation '$identifier' with table $tableIdentifier by adding new columns ${missingFields.map(_.name).mkString(",")}. Final schema is\n ${newSchema.treeString}")
                    catalog.addTableColumns(tableIdentifier, missingFields)
                }
            }
        }
    }

    /**
      * Applies the specified schema and converts all field names to lowercase. This is required when directly
      * writing into HDFS and using Hive, since Hive only supports lower-case field names.
      *
      * @param df
      * @return
      */
    override protected def applyOutputSchema(executor:Executor, df: DataFrame) : DataFrame = {
        val outputSchema = Some(executor.catalog.getTable(tableIdentifier).dataSchema)
        val mixedCaseDf = SchemaUtils.applySchema(df, outputSchema)
        if (needsLowerCaseSchema) {
            val lowerCaseSchema = SchemaUtils.toLowerCase(mixedCaseDf.schema)
            df.sparkSession.createDataFrame(mixedCaseDf.rdd, lowerCaseSchema)
        }
        else {
            mixedCaseDf
        }
    }

    private def needsLowerCaseSchema : Boolean = {
        false
    }
}





class HiveTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = true) private var table: String = ""
    @JsonProperty(value = "external", required = false) private var external: String = "false"
    @JsonProperty(value = "location", required = false) private var location: Option[String] = None
    @JsonProperty(value = "format", required = false) private var format: Option[String] = None
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value = "rowFormat", required = false) private var rowFormat: Option[String] = None
    @JsonProperty(value = "inputFormat", required = false) private var inputFormat: Option[String] = None
    @JsonProperty(value = "outputFormat", required = false) private var outputFormat: Option[String] = None
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "serdeProperties", required = false) private var serdeProperties: Map[String, String] = Map()
    @JsonProperty(value = "writer", required = false) private var writer: String = "hive"

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): Relation = {
        HiveTableRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.evaluate(database),
            context.evaluate(table),
            context.evaluate(external).toBoolean,
            context.evaluate(location).map(p => new Path(context.evaluate(p))),
            context.evaluate(format),
            context.evaluate(options),
            context.evaluate(rowFormat),
            context.evaluate(inputFormat),
            context.evaluate(outputFormat),
            context.evaluate(properties),
            context.evaluate(serdeProperties),
            context.evaluate(writer).toLowerCase(Locale.ROOT)
        )
    }
}
