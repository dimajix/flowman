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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.IncompatibleSchemaException
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


object HiveTableRelation {
    val AVRO_SCHEMA_URL = "avro.schema.url"
}


class HiveTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    override val database: Option[String],
    override val table: String,
    val external: Boolean,
    val location: Option[Path],
    val format: String,
    val rowFormat: Option[String],
    val inputFormat: Option[String],
    val outputFormat: Option[String],
    val properties: Map[String, String],
    val serdeProperties: Map[String, String],
    val writer: String
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
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: String): Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

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

        val partitionNames = partitions.map(_.name)
        logger.info(s"Writing to Hive table '$tableIdentifier' with partitions ${partitionNames.mkString(",")} using Hive insert")

        // Apply output schema before writing to Hive
        val outputDf = applyOutputSchema(executor, df)

        if (partitionSpec.nonEmpty) {
            val spark = executor.spark
            val catalog = executor.catalog
            val hiveTable = catalog.getTable(TableIdentifier(table, database))
            val query = outputDf.queryExecution.logical

            val overwrite = mode.toLowerCase(Locale.ROOT) == "overwrite"
            val cmd = InsertIntoHiveTable(
                table = hiveTable,
                partition = partitionSpec.toMap.mapValues(v => Some(v.toString)),
                query = query,
                overwrite = overwrite,
                ifPartitionNotExists = false,
                query.output.map(_.name)
            )
            val qe = spark.sessionState.executePlan(cmd)
            SQLExecution.withNewExecutionId(spark, qe)(qe.toRdd)

            // Finally add Hive partition
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

        logger.info(s"Writing to Hive table '$tableIdentifier' with partition values $partitionSpec using direct mode")

        if (location.isEmpty)
            throw new IllegalArgumentException("Hive table relation requires 'location' for direct write mode")

        val outputPath = partitionSpec.path(location.get, partitions.map(_.name))

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
    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        logger.info(s"Cleaning Hive table '$tableIdentifier' for relatio '$identifier'")

        val catalog = executor.catalog
        // When no partitions are specified, this implies that the whole table is to be truncated
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
      * Creates a Hive table by executing the appropriate DDL
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists:Boolean=false): Unit = {
        require(executor != null)

        if (!ifNotExists || !exists(executor)) {
            logger.info(s"Creating Hive table '$tableIdentifier' for relation '$identifier'")

            // Create and save Avro schema
            import HiveTableRelation._
            if (properties.contains(AVRO_SCHEMA_URL)) {
                val avroSchemaUrl = properties(AVRO_SCHEMA_URL)
                logger.info(s"Storing Avro schema at location $avroSchemaUrl")
                new SchemaWriter(fields)
                    .format("avro")
                    .save(executor.fs.file(avroSchemaUrl))
            }

            val defaultStorage = HiveSerDe.getDefaultStorage(executor.spark.sessionState.conf)
            val fileStorage: CatalogStorageFormat = if (format != null && format.nonEmpty) {
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
                schema = StructType(fields.map(_.sparkField) ++ partitions.map(_.sparkField)),
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
            logger.info(s"Destroying Hive table '$tableIdentifier' of relation '$identifier'")
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
        val sourceSchema = schema.get.sparkSchema

        val table = catalog.getTable(tableIdentifier)
        val targetSchema = table.dataSchema
        val targetFields = targetSchema.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap

        // Ensure that current real Hive schema is compatible with specified schema
        val isCompatible = sourceSchema.forall { field =>
            targetFields.get(field.name.toLowerCase(Locale.ROOT))
                .forall(tgt => SchemaUtils.isCompatible(field, tgt))
        }
        if (!isCompatible) {
            throw new IncompatibleSchemaException(identifier)
        }

        val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name.toLowerCase(Locale.ROOT)))
        if (missingFields.nonEmpty) {
            logger.info(s"Migrating HiveTable relation '$identifier' by adding new columns ${missingFields.map(_.name).mkString(",")} to Hive table '$tableIdentifier'")
            catalog.addTableColumns(tableIdentifier, missingFields)
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
    @JsonProperty(value = "format", required = false) private var format: String = _
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
        new HiveTableRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.evaluate(database),
            context.evaluate(table),
            context.evaluate(external).toBoolean,
            context.evaluate(location).map(p => new Path(context.evaluate(p))),
            context.evaluate(format),
            context.evaluate(rowFormat),
            context.evaluate(inputFormat),
            context.evaluate(outputFormat),
            context.evaluate(properties),
            context.evaluate(serdeProperties),
            context.evaluate(writer).toLowerCase(Locale.ROOT)
        )
    }
}
