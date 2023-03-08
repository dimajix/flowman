/*
 * Copyright (C) 2021 The Flowman Authors
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

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.catalog.TableChange
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
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


case class DeltaTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    table: TableIdentifier,
    location: Option[Path] = None,
    options: Map[String,String] = Map.empty,
    properties: Map[String, String] = Map.empty,
    mergeKey: Seq[String] = Seq.empty,
    override val migrationPolicy: MigrationPolicy = MigrationPolicy.RELAXED,
    override val migrationStrategy: MigrationStrategy = MigrationStrategy.ALTER
) extends DeltaRelation(options, mergeKey) {
    private val logger = LoggerFactory.getLogger(classOf[DeltaTableRelation])
    protected val resource = ResourceIdentifier.ofHiveTable(table)

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
            case Operation.CREATE | Operation.DESTROY => table.space.headOption.map(ResourceIdentifier.ofHiveDatabase).toSet
            case Operation.READ =>
                requireValidPartitionKeys(partitions)

                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(table, p.toMap)).toSet ++
                    Set(resource)
            case Operation.WRITE =>
                Set(resource)
        }
        deps ++ super.requires(op, partitions)
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param schema     - the schema to read. If none is specified, all available columns will be read
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        logger.info(s"Reading Delta relation '$identifier' from table $table using partition values $partitions")

        val tableDf = execution.spark.read
            .options(options)
            .table(table.quotedString)

        val filteredDf = filterPartition(tableDf, partitions)
        applyInputSchema(execution, filteredDf)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df        - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        requireAllPartitionKeys(partition, df.columns)

        val partitionSpec = PartitionSchema(partitions).spec(partition)

        logger.info(s"Writing Delta relation '$identifier' to table $table partition ${HiveDialect.expr.partition(partitionSpec)} with mode '$mode'")

        val extDf = applyOutputSchema(execution, addPartition(df, partition))
        mode match {
            case OutputMode.UPDATE => doUpdate(extDf, partitionSpec)
            case _ => doWrite(extDf, partitionSpec, mode)
        }

        execution.catalog.refreshTable(table)
    }
    private def doWrite(df: DataFrame, partitionSpec: PartitionSpec, mode: OutputMode) : Unit = {
        val writer =
            if (partitionSpec.nonEmpty || mode == OutputMode.OVERWRITE_DYNAMIC) {
                df.write
                    .option("replaceWhere", replaceWhere(df, partitionSpec, mode))
            }
            else {
                df.write
            }

        writer
            .format("delta")
            .options(options)
            .mode(mode.batchMode)
            .insertInto(table.quotedString)
    }
    private def doUpdate(df: DataFrame, partitionSpec: PartitionSpec) : Unit = {
        val withinPartitionKeyColumns = if (mergeKey.nonEmpty) mergeKey else schema.map(_.primaryKey).getOrElse(Seq())
        val keyColumns = SetIgnoreCase(partitions.map(_.name)) -- partitionSpec.keys ++ withinPartitionKeyColumns
        val table = DeltaTable.forName(df.sparkSession, this.table.quotedString)
        DeltaUtils.upsert(table, df, keyColumns, partitionSpec)
    }

    /**
     * Reads data from a streaming source
     *
     * @param execution
     * @param schema
     * @return
     */
    override def readStream(execution: Execution): DataFrame = {
        logger.info(s"Streaming from Delta table relation '$identifier' at $table")
        val location = DeltaUtils.getLocation(execution, table.toSpark)
        readStreamFrom(execution, location)
    }

    /**
     * Writes data to a streaming sink
     *
     * @param execution
     * @param df
     * @return
     */
    override def writeStream(execution: Execution, df: DataFrame, mode: OutputMode, trigger: Trigger, checkpointLocation: Path): StreamingQuery = {
        logger.info(s"Streaming to Delta table relation '$identifier' $table")
        val location = DeltaUtils.getLocation(execution, table.toSpark)
        writeStreamTo(execution, df, location, mode, trigger, checkpointLocation)
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
     * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     * [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
     *
     * @param execution
     * @return
     */
    override def exists(execution: Execution): Trilean = {
        execution.catalog.tableExists(table)
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
            fullSchema match {
                case Some(fullSchema) =>
                    val table = catalog.getTable(this.table)
                    if (table.tableType == CatalogTableType.VIEW) {
                        false
                    }
                    else  {
                        val table = deltaCatalogTable(execution)
                        val sourceSchema = com.dimajix.flowman.types.StructType.of(table.schema())
                        val targetSchema = com.dimajix.flowman.types.SchemaUtils.replaceCharVarchar(fullSchema)
                        val sourceTable = TableDefinition(
                            this.table,
                            TableType.TABLE,
                            columns = sourceSchema.fields,
                            partitionColumnNames = table.snapshot.metadata.partitionColumns
                        )
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
        if (!catalog.tableExists(table)) {
            false
        }
        else if (partitions.nonEmpty) {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            DeltaUtils.isLoaded(execution, table.toSpark, partitionSpec)
        }
        else {
            val location = catalog.getTableLocation(table)
            DeltaUtils.isLoaded(execution, location)
        }
    }

    /**
     * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
     * relation will not contain any data, but all metadata will be processed
     *
     * @param execution
     */
    override def create(execution: Execution): Unit = {
        val sparkSchema = HiveCatalog.cleanupSchema(StructType(fields.map(_.catalogField)))
        logger.info(s"Creating Delta table relation '$identifier' with table $table and schema\n${sparkSchema.treeString}")
        if (schema.isEmpty) {
            throw new UnspecifiedSchemaException(identifier)
        }

        val tableExists = exists(execution) == Yes
        if (tableExists)
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)

        DeltaUtils.createTable(
            execution,
            Some(table.toSpark),
            location,
            sparkSchema,
            partitions,
            properties,
            description
        )

        execution.refreshResource(resource)
    }

    /**
     * Removes one or more partitions.
     *
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        requireValidPartitionKeys(partitions)

        if (partitions.nonEmpty) {
            val deltaTable = DeltaTable.forName(execution.spark, table.quotedString)
            PartitionSchema(this.partitions).interpolate(partitions).foreach { p =>
                deltaTable.delete(p.predicate)
            }
            deltaTable.vacuum()
        }
        else {
            logger.info(s"Truncating Delta table relation '$identifier' by truncating table $table")
            val deltaTable = DeltaTable.forName(execution.spark, table.quotedString)
            deltaTable.delete()
            deltaTable.vacuum()
        }
    }

    /**
     * This will delete any physical representation of the relation. Depending on the type only some meta data like
     * a Hive table might be dropped or also the physical files might be deleted
     *
     * @param execution
     */
    override def destroy(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Destroying Delta table relation '$identifier' by dropping table $table")
        val catalog = execution.catalog
        catalog.dropTable(table)
        execution.refreshResource(resource)
    }

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution): Unit = {
        require(execution != null)

        val catalog = execution.catalog
        if (catalog.tableExists(table)) {
            val table = catalog.getTable(this.table)
            if (table.tableType == CatalogTableType.VIEW) {
                migrationStrategy match {
                    case MigrationStrategy.NEVER =>
                        logger.warn(s"Migration required for HiveTable relation '$identifier' from VIEW to a TABLE $this.table, but migrations are disabled.")
                    case MigrationStrategy.FAIL =>
                        logger.error(s"Cannot migrate relation HiveTable '$identifier' from VIEW to a TABLE $this.table, since migrations are disabled.")
                        throw new MigrationFailedException(identifier)
                    case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                        logger.warn(s"TABLE target $this.table is currently a VIEW, dropping...")
                        catalog.dropView(this.table, false)
                        create(execution)
                }
            }
            else if (schema.nonEmpty) {
                migrateInternal(execution)
            }
        }
    }

    override protected def deltaTable(execution: Execution) : DeltaTable = {
        DeltaTable.forName(execution.spark, table.quotedString)
    }

    override protected def deltaCatalogTable(execution: Execution): DeltaTableV2 = {
        val catalog = execution.catalog
        val catalogTable = catalog.getTable(table)

        DeltaTableV2(
            execution.spark,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(table.unquotedString)
        )
    }
}


@RelationType(kind="deltaTable")
class DeltaTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec with MigratableRelationSpec {
    @JsonProperty(value = "database", required = false) private var database: Option[String] = Some("default")
    @JsonProperty(value = "table", required = true) private var table: String = ""
    @JsonProperty(value = "location", required = false) private var location: Option[String] = None
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq()

    override def instantiate(context: Context, props:Option[Relation.Properties] = None): DeltaTableRelation = {
        DeltaTableRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            TableIdentifier(context.evaluate(table), context.evaluate(database)),
            context.evaluate(location).map(p => new Path(p)),
            context.evaluate(options),
            context.evaluate(properties),
            mergeKey.map(context.evaluate),
            evalMigrationPolicy(context),
            evalMigrationStrategy(context)
        )
    }
}
