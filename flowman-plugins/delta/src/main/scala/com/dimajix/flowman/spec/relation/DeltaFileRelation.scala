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

import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.UnspecifiedSchemaException
import com.dimajix.flowman.fs.HadoopUtils
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.DataFrameUtils.withTempView


final case class DeltaFileRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    location: Path,
    options: Map[String,String] = Map.empty,
    properties: Map[String, String] = Map.empty,
    mergeKey: Seq[String] = Seq.empty,
    override val migrationPolicy: MigrationPolicy = MigrationPolicy.RELAXED,
    override val migrationStrategy: MigrationStrategy = MigrationStrategy.ALTER
) extends DeltaRelation(options, mergeKey) {
    protected val resource = ResourceIdentifier.ofFile(location)

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
                if (this.partitions.nonEmpty) {
                    // TODO: Get all partition paths
                    //val allPartitions = PartitionSchema(this.partitions).interpolate(partition)
                    //allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p))).toSet
                    Set(resource)
                }
                else {
                    Set(resource)
                }
        }
    }

    /**
     * Returns the list of all resources which will be required by this relation for creation.
     *
     * @return
     */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] =  {
        val deps = op match {
            case Operation.CREATE | Operation.DESTROY => Set.empty
            case Operation.READ =>
                requireValidPartitionKeys(partitions)
                if (this.partitions.nonEmpty) {
                    // TODO: Get all partition paths
                    //val allPartitions = PartitionSchema(this.partitions).interpolate(partition)
                    //allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p))).toSet
                    Set(resource)
                }
                else {
                    Set(resource)
                }
            case Operation.WRITE => Set.empty
        }
        deps ++ super.requires(op, partitions)
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        logger.info(s"Reading Delta file relation '$identifier' at '$location' using partition values $partitions")

        val tableDf = execution.spark.read
            .options(options)
            .format("delta")
            .load(location.toString)

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

        logger.info(s"Writing Delta file relation '$identifier' partition ${HiveDialect.expr.partition(partitionSpec)} to location '$location' with mode '$mode'")

        val extDf = applyOutputSchema(execution, addPartition(df, partition))
        mode match {
            case OutputMode.UPDATE => doUpdate(extDf, partitionSpec)
            case _ => doWrite(extDf, partitionSpec, mode)
        }
    }
    private def doWrite(df: DataFrame, partitionSpec: PartitionSpec, mode: OutputMode) : Unit = {
        val writer =
            if (partitionSpec.nonEmpty || mode == OutputMode.OVERWRITE_DYNAMIC) {
                df.write
                    .partitionBy(this.partitions.map(_.name):_*)
                    .option("replaceWhere", replaceWhere(df, partitionSpec, mode))
            }
            else {
                df.write
            }

        writer
            .format("delta")
            .options(options)
            .mode(mode.batchMode)
            .save(location.toString)
    }
    private def doUpdate(df: DataFrame, partitionSpec: PartitionSpec) : Unit = {
        val withinPartitionKeyColumns = if (mergeKey.nonEmpty) mergeKey else schema.map(_.primaryKey).getOrElse(Seq())
        val keyColumns = SetIgnoreCase(partitions.map(_.name)) -- partitionSpec.keys ++ withinPartitionKeyColumns
        val table = DeltaTable.forPath(df.sparkSession, location.toString)
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
        logger.info(s"Streaming from Delta file relation '$identifier' at '$location'")
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
        logger.info(s"Streaming to Delta file relation '$identifier' at '$location'")
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
        DeltaTable.isDeltaTable(execution.spark, location.toString)
    }


    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = {
        if (exists(execution) == Yes) {
            fullSchema match {
                case Some(fullSchema) =>
                    val table = deltaCatalogTable(execution)
                    val sourceSchema = com.dimajix.flowman.types.StructType.of(table.schema())
                    val targetSchema = com.dimajix.flowman.types.SchemaUtils.replaceCharVarchar(fullSchema)
                    val sourceTable = TableDefinition(
                        TableIdentifier.empty,
                        columns = sourceSchema.fields,
                        partitionColumnNames = table.snapshot.metadata.partitionColumns
                    )
                    val targetTable = TableDefinition(
                        TableIdentifier.empty,
                        columns = targetSchema.fields,
                        partitionColumnNames = partitions.map(_.name)
                    )
                    !TableChange.requiresMigration(sourceTable, targetTable, migrationPolicy)
                case None => true
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

        if (!DeltaTable.isDeltaTable(execution.spark, location.toString)) {
            false
        }
        else if (partition.nonEmpty) {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            DeltaUtils.isLoaded(execution, location, partitionSpec)
        }
        else {
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
        val sparkSchema = StructType(fields.map(_.catalogField))
        logger.info(s"Creating Delta file relation '$identifier' at '$location' and schema\n${sparkSchema.treeString}")
        if (schema.isEmpty) {
            throw new UnspecifiedSchemaException(identifier)
        }

        val tableExists = exists(execution) == Yes
        if (tableExists)
            throw new FileAlreadyExistsException(s"Delta file relation at at '$location' already exists")

        DeltaUtils.createTable(
            execution,
            None,
            Some(location),
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
            val deltaTable = DeltaTable.forPath(execution.spark, location.toString)
            PartitionSchema(this.partitions).interpolate(partitions).foreach { p =>
                deltaTable.delete(p.predicate)
            }
            deltaTable.vacuum()
        }
        else {
            val fs = location.getFileSystem(execution.hadoopConf)
            HadoopUtils.truncateLocation(fs, location)

            // TODO: Pickup existing physical schema
            val sparkSchema = StructType(fields.map(_.catalogField))
            DeltaUtils.createTable(
                execution,
                None,
                Some(location),
                sparkSchema,
                this.partitions,
                properties,
                description
            )
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

        val location = this.location
        val fs = location.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (!fs.exists(location)) {
            throw new FileNotFoundException(location.toString)
        }
        else {
            logger.info(s"Destroying Delta file relation '$identifier' by deleting directory '$location'")
            fs.delete(location, true)
            execution.refreshResource(resource)
        }
    }

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution): Unit = {
        require(execution != null)

        // Only perform migration when the schema is defined and when the relation actually exists
        if (schema.nonEmpty && exists(execution) == Yes) {
            migrateInternal(execution)
        }
    }

    override protected def deltaTable(execution: Execution) : DeltaTable = {
        DeltaTable.forPath(execution.spark, location.toString)
    }

    override protected def deltaCatalogTable(execution: Execution): DeltaTableV2 = {
        DeltaTableV2(execution.spark, location)
    }
}



@RelationType(kind="deltaFile")
class DeltaFileRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec with MigratableRelationSpec {
    @JsonProperty(value = "location", required = false) private var location: String = ""
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq()

    override def instantiate(context: Context, props:Option[Relation.Properties] = None): DeltaFileRelation = {
        DeltaFileRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            new Path(context.evaluate(location)),
            context.evaluate(options),
            context.evaluate(properties),
            mergeKey.map(context.evaluate),
            evalMigrationPolicy(context),
            evalMigrationStrategy(context)
        )
    }
}
