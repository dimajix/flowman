/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.util.Locale
import scala.util.control.NonFatal
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.PartitionChange
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.ChangeStorageFormat
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.UnspecifiedSchemaException
import com.dimajix.flowman.fs.{File, HadoopUtils}
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.MigratableRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.features.hiveVarcharSupported
import com.dimajix.spark.sql.{SchemaUtils => SparkSchemaUtils}


object HiveTableRelation {
    val AVRO_SCHEMA_URL = "avro.schema.url"
}


final case class HiveTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    override val table: TableIdentifier,
    external: Boolean = false,
    location: Option[Path] = None,
    format: Option[String] = None,
    options: Map[String,String] = Map.empty,
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map.empty,
    serdeProperties: Map[String, String] = Map.empty,
    writer: String = "hive",
    override val migrationPolicy: MigrationPolicy = MigrationPolicy.RELAXED,
    override val migrationStrategy: MigrationStrategy = MigrationStrategy.ALTER
) extends HiveRelation with SchemaRelation with MigratableRelation {
    private val resource = ResourceIdentifier.ofHiveTable(table)

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY => Set(resource)
            case Operation.READ => Set.empty
            case Operation.WRITE =>
                requireValidPartitionKeys(partitions)

                // Only return Hive table partitions!
                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(table, p.toMap)).toSet
        }
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        val deps = op match {
            case Operation.CREATE|Operation.DESTROY =>
                table.database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
            case Operation.READ =>
                requireValidPartitionKeys(partitions)

                // Only return Hive table partitions!
                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(table, p.toMap)).toSet ++ Set(resource)
            case Operation.WRITE =>
                Set(resource)
        }
        deps ++ super.requires(op, partitions)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode:OutputMode = OutputMode.OVERWRITE): Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        //requireAllPartitionKeys(partition)

        val schema = PartitionSchema(partitions)
        val partitionSpec = schema.spec(partition)

        if (writer == "hive")
            writeHive(execution, df, partitionSpec, mode)
        else if (writer == "spark")
            writeSpark(execution, df, partitionSpec, mode)
        else
            throw new IllegalArgumentException("Hive relations only support write modes 'hive' and 'spark'")

        execution.refreshResource(resource)
    }

    /**
      * Writes to a Hive table using Hive. This is the normal mode.
      *
      * @param executor
      * @param df
      * @param partitionSpec
      * @param mode
      */
    private def writeHive(execution: Execution, df: DataFrame, partitionSpec: PartitionSpec, mode:OutputMode): Unit = {
        require(execution != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        logger.info(s"Writing Hive relation '$identifier' to table $table partition ${partitionSpec.spec} with mode '$mode' using Hive insert")

        // Apply output schema before writing to Hive
        val outputDf = {
            if (partitionSpec.isEmpty)
                applyOutputSchema(execution, df, includePartitions = true)
            else
                applyOutputSchema(execution, df)
        }

        def loaded() : Boolean = {
            val catalog = execution.catalog
            if (partitionSpec.nonEmpty) {
                catalog.partitionExists(table, partitionSpec)
            }
            else {
                val location = catalog.getTableLocation(table)
                val fs = location.getFileSystem(execution.hadoopConf)
                HadoopUtils.isValidHiveData(fs, location)
            }
        }

        mode match {
            case OutputMode.APPEND|OutputMode.OVERWRITE|OutputMode.OVERWRITE_DYNAMIC =>
                writeHiveTable(execution, outputDf, partitionSpec, mode)
            case OutputMode.IGNORE_IF_EXISTS =>
                if (!loaded()) {
                    writeHiveTable(execution, outputDf, partitionSpec, mode)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (loaded()) {
                    if (partitionSpec.nonEmpty)
                        throw new PartitionAlreadyExistsException(table.database.getOrElse(""), table.table, partitionSpec.mapValues(_.toString).toMap)
                    else
                        throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
                }
                writeHiveTable(execution, outputDf, partitionSpec, mode)
            case _ =>
                throw new IllegalArgumentException(s"Unsupported output mode '$mode' for Hive table relation $identifier")
        }
    }

    private def writeHiveTable(execution: Execution, df:DataFrame, partitionSpec: PartitionSpec, mode: OutputMode) : Unit = {
        val spark = execution.spark
        val catalog = execution.catalog

        if (partitionSpec.nonEmpty) {
            val hiveTable = catalog.getTable(table)
            val query = df.queryExecution.logical

            val overwrite = mode == OutputMode.OVERWRITE || mode == OutputMode.OVERWRITE_DYNAMIC
            val cmd = InsertIntoHiveTable(
                table = hiveTable,
                partition = partitionSpec.catalogPartition.mapValues(Some(_)),
                query = query,
                overwrite = overwrite,
                ifPartitionNotExists = false,
                query.output.map(_.name)
            )
            val qe = spark.sessionState.executePlan(cmd)
            SparkShim.withNewExecutionId(spark, qe)(qe.toRdd)

            // Finally refresh Hive partition
            catalog.refreshPartition(table, partitionSpec)
        }
        else {
            // If OVERWRITE is specified, remove all partitions for partitioned tables
            if (partitions.nonEmpty && mode == OutputMode.OVERWRITE) {
                catalog.truncateTable(table)
            }
            val writer = df.write
                .mode(mode.batchMode)
                .options(options)
            format.foreach(writer.format)
            writer.insertInto(table.unquotedString)

            execution.catalog.refreshTable(table)
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
    private def writeSpark(execution: Execution, df: DataFrame, partitionSpec: PartitionSpec, mode:OutputMode): Unit = {
        require(execution != null)
        require(df != null)
        require(partitionSpec != null)
        require(mode != null)

        logger.info(s"Writing Hive relation '$identifier' to table $table partition ${partitionSpec.spec} with mode '$mode' using direct write")

        if (location.isEmpty)
            throw new IllegalArgumentException("Hive table relation requires 'location' for direct write mode")

        val outputPath = partitionSpec.path(location.get, partitions.map(_.name))

        // Perform Hive => Spark format mapping
        val format = this.format.map(_.toLowerCase(Locale.ROOT)) match {
            case Some("avro") => "com.databricks.spark.avro"
            case Some(f) => f
            case None => throw new IllegalArgumentException("Require 'format' for directly writing to Hive tables")
        }

        logger.info(s"Writing to output location '$outputPath' (partition=${partitionSpec.spec}) as '$format'")
        this.writer(execution, df, format, options, mode.batchMode)
            .save(outputPath.toString)

        // Finally add Hive partition
        val catalog = execution.catalog
        if (partitionSpec.nonEmpty) {
            catalog.addOrReplacePartition(table, partitionSpec, outputPath)
        }
        else {
            catalog.refreshTable(table)
        }
    }

    /**
      * Cleans either individual partitions (for partitioned tables) or truncates a whole table
      *
      * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val catalog = execution.catalog
        // When no partitions are specified, this implies that the whole table is to be truncated
        if (partitions.nonEmpty) {
            val partitionSchema = PartitionSchema(this.partitions)
            partitionSchema.interpolate(partitions).foreach { spec =>
                logger.info(s"Truncating Hive relation '$identifier' by truncating table $table partition ${HiveDialect.expr.partition(spec)}")
                catalog.dropPartition(table, spec)
            }
        }
        else {
            logger.info(s"Truncating Hive relation '$identifier' by truncating table $table")
            catalog.truncateTable(table)
        }
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = {
        val catalog = execution.catalog
        if (catalog.tableExists(table)) {
            val table = catalog.getTable(this.table)
            val locationExists = table.storage.locationUri.forall(loc => execution.fs.file(loc).exists())
            if (!locationExists) {
                false
            }
            else {
                fullSchema match {
                    case Some(fullSchema) =>
                        if (table.tableType == CatalogTableType.VIEW) {
                            false
                        }
                        else {
                            val sourceTable = TableDefinition.ofTable(table)
                            val targetSchema = com.dimajix.flowman.types.StructType(fullSchema.fields)
                            val targetTable = TableDefinition(
                                this.table,
                                TableType.TABLE,
                                columns = targetSchema.fields,
                                partitionColumnNames = partitions.map(_.name)
                            )

                            !TableChange.requiresMigration(sourceTable, targetTable, migrationPolicy)
                        }
                    case None =>
                        true
                }
            }
        }
        else {
            false
        }
    }

    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        require(execution != null)
        require(partition != null)

        requireValidPartitionKeys(partition)

        val catalog = execution.catalog
        if (partitions.nonEmpty) {
            val schema = PartitionSchema(partitions)
            val partitionSpec = schema.spec(partition)
            catalog.tableExists(table) &&
                catalog.partitionExists(table, partitionSpec)
        }
        else {
            if (catalog.tableExists(table)) {
                val location = catalog.getTableLocation(table)
                val fs = location.getFileSystem(execution.hadoopConf)
                HadoopUtils.isValidHiveData(fs, location)
            }
            else {
                No
            }
        }
    }

    /**
      * Creates a Hive table by executing the appropriate DDL
      *
      * @param execution
      */
    override def create(execution: Execution): Unit = {
        require(execution != null)

        val catalogSchema = StructType(fields.map(_.catalogField))
        logger.info(s"Creating Hive table relation '$identifier' with table $table and schema\n${catalogSchema.treeString}")
        if (schema.isEmpty) {
            throw new UnspecifiedSchemaException(identifier)
        }

        // Create and save Avro schema
        import HiveTableRelation._
        if (properties.contains(AVRO_SCHEMA_URL)) {
            val avroSchemaUrl = properties(AVRO_SCHEMA_URL)
            logger.info(s"Storing Avro schema at location $avroSchemaUrl")
            new SchemaWriter(schema.toSeq.flatMap(_.fields))
                .format("avro")
                .save(execution.fs.file(avroSchemaUrl))
        }

        val defaultStorage = HiveSerDe.getDefaultStorage(execution.spark.sessionState.conf)
        val fileStorage: CatalogStorageFormat = if (format.exists(_.nonEmpty)) {
            HiveSerDe.sourceToSerDe(format.get) match {
                case Some(s) =>
                    CatalogStorageFormat.empty.copy(
                        inputFormat = s.inputFormat,
                        outputFormat = s.outputFormat,
                        serde = s.serde)
                case None =>
                    throw new IllegalArgumentException(s"File format '$format' not supported in Hive relation '$identifier' while creating hive table $table")
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
        val rowFormat = this.rowFormat
            .filter(_.nonEmpty)
            .orElse(fileStorage.serde)
            .orElse(defaultStorage.serde)

        // Configure catalog table by assembling all options
        val catalogTable = CatalogTable(
            identifier = table.toSpark,
            tableType =
                if (external)
                    CatalogTableType.EXTERNAL
                else
                    CatalogTableType.MANAGED,
            storage = CatalogStorageFormat(
                locationUri = location.map(_.toUri),
                inputFormat = inputFormat,
                outputFormat = outputFormat,
                serde = rowFormat,
                compressed = false,
                properties = fileStorage.properties ++ serdeProperties
            ),
            provider = Some("hive"),
            schema = catalogSchema,
            partitionColumnNames = partitions.map(_.name),
            properties = properties,
            comment = description
        )

        // Create table
        val catalog = execution.catalog
        catalog.createTable(catalogTable, false)
        execution.refreshResource(resource)
    }

    /**
      * Destroys the Hive table by executing an appropriate DROP statement
      *
      * @param execution
      */
    override def destroy(execution: Execution): Unit = {
        require(execution != null)

        val catalog = execution.catalog
        logger.info(s"Destroying Hive table relation '$identifier' by dropping table $table")
        catalog.dropTable(table)
        execution.refreshResource(resource)
    }

    /**
      * Performs migration of a Hive table by adding new columns
      * @param execution
      */
    override def migrate(execution: Execution): Unit = {
        require(execution != null)

        val catalog = execution.catalog
        if (schema.nonEmpty && catalog.tableExists(table)) {
            val curTable = catalog.getTable(table)
            // Check if current table is a VIEW or a TABLE
            if (curTable.tableType == CatalogTableType.VIEW) {
                migrateFromView(execution)
            }
            else {
                // Ensure target directory exists
                val location = execution.fs.file(curTable.location)
                if (!location.exists()) {
                    location.mkdirs()
                }
                migrateFromTable(execution, curTable)
            }
        }
    }

    private def migrateFromView(execution:Execution) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for HiveTable relation '$identifier' from VIEW to a TABLE ${table}, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate relation HiveTable '$identifier' from VIEW to a TABLE ${table}, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                logger.warn(s"TABLE target ${table} is currently a VIEW, dropping...")
                val catalog = execution.catalog
                catalog.dropView(table, false)
                create(execution)
                execution.refreshResource(resource)
        }
    }

    private def migrateFromTable(execution:Execution, curTable:CatalogTable) : Unit = {
        val sourceTable = TableDefinition.ofTable(curTable)
        val targetSchema =  com.dimajix.flowman.types.StructType(fullSchema.get.fields)
        val targetTable = TableDefinition(
            table,
            TableType.TABLE,
            columns = targetSchema.fields,
            partitionColumnNames = partitions.map(_.name)
        )

        val requiresMigration = TableChange.requiresMigration(sourceTable, targetTable, migrationPolicy)
        if (requiresMigration) {
            doMigration(execution, sourceTable, targetTable)
        }
    }

    private def doMigration(execution: Execution, currentTable:TableDefinition, targetTable:TableDefinition) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for HiveTable relation '$identifier' of Hive table $table, but migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate relation HiveTable '$identifier' of Hive table $table, since migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER =>
                val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
                if (migrations.exists(m => !supported(m))) {
                    val unsupportedChanges = migrations.filter(m => !supported(m))
                    logger.error(s"Cannot migrate relation HiveTable '$identifier' of Hive table $table, since that would require unsupported changes.\n" +
                        s"Current schema:\n${currentTable.schema.treeString}" +
                        s"New schema:\n${targetTable.schema.treeString}" +
                        s"Unsupported changes:\n${unsupportedChanges.map(m => " - " + m.toString).mkString("\n")}"
                    )
                    throw new MigrationFailedException(identifier)
                }
                alter(migrations)
            case MigrationStrategy.ALTER_REPLACE =>
                val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
                if (migrations.forall(m => supported(m))) {
                    alter(migrations)
                }
                else {
                    recreate()
                }
            case MigrationStrategy.REPLACE =>
                recreate()
        }

        def alter(migrations:Seq[TableChange]) : Unit = {
            logger.info(s"Migrating HiveTable relation '$identifier', this will alter the Hive table $table. New schema:\n${targetTable.schema.treeString}")
            if (migrations.isEmpty) {
                logger.warn("Empty list of migrations - nothing to do")
            }
            else {
                try {
                    execution.catalog.alterTable(table, migrations)
                    execution.refreshResource(resource)
                }
                catch {
                    case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
                }
            }
        }

        def recreate() : Unit = {
            logger.info(s"Migrating HiveTable relation '$identifier', this will drop/create the Hive table $table.")
            try {
                destroy(execution)
                create(execution)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def supported(change:TableChange) : Boolean = {
            change match {
                case _:DropColumn => false
                case a:AddColumn => a.column.nullable // Only allow nullable columns to be added
                case _:UpdateColumnNullability => true
                case _:UpdateColumnType => false
                case _:UpdateColumnComment => true
                case _:PartitionChange => false
                case _:ChangeStorageFormat => false
                case x:TableChange => throw new UnsupportedOperationException(s"Table change ${x} not supported")
            }
        }
    }

    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        // We specifically use the existing physical Hive schema
        val currentSchema = execution.catalog.getTable(table).dataSchema

        // If a schema is explicitly specified, we use that one to back-merge VarChar(n) and Char(n). This
        // is mainly required for Spark < 3.1, which cannot correctly handle VARCHAR and CHAR types in Hive
        if (!hiveVarcharSupported) {
            schema.map { schema =>
                val desiredSchema = MapIgnoreCase(schema.catalogSchema.map(f => f.name -> f))
                val mergedFields = currentSchema.map { field =>
                    field.dataType match {
                        case StringType =>
                            desiredSchema.get(field.name).map { dfield =>
                                dfield.dataType match {
                                    case VarcharType(n) => field.copy(dataType = VarcharType(n))
                                    case CharType(n) => field.copy(dataType = CharType(n))
                                    case _ => field
                                }
                            }.getOrElse(field)
                        case _ => field
                    }
                }
                StructType(mergedFields)
            }.orElse(Some(currentSchema))
        }
        else {
            Some(currentSchema)
        }
    }

    /**
      * Applies the specified schema and converts all field names to lowercase. This is required when directly
      * writing into HDFS and using Hive, since Hive only supports lower-case field names.
      *
      * @param df
      * @return
      */
    override protected def applyOutputSchema(execution:Execution, df: DataFrame, includePartitions:Boolean=false) : DataFrame = {
        val mixedCaseDf = super.applyOutputSchema(execution, df, includePartitions)
        if (needsLowerCaseSchema) {
            val lowerCaseSchema = SparkSchemaUtils.toLowerCase(mixedCaseDf.schema)
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





class HiveTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec with MigratableRelationSpec {
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
    override def instantiate(context: Context, props:Option[Relation.Properties] = None): Relation = {
        HiveTableRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            TableIdentifier(context.evaluate(table), context.evaluate(database).toSeq),
            context.evaluate(external).toBoolean,
            context.evaluate(location).map(p => new Path(p)),
            context.evaluate(format),
            context.evaluate(options),
            context.evaluate(rowFormat),
            context.evaluate(inputFormat),
            context.evaluate(outputFormat),
            context.evaluate(properties),
            context.evaluate(serdeProperties),
            context.evaluate(writer).toLowerCase(Locale.ROOT),
            evalMigrationPolicy(context),
            evalMigrationStrategy(context)
        )
    }
}
