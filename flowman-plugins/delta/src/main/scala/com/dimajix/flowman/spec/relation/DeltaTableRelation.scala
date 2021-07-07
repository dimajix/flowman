/*
 * Copyright 2021 Kaya Kupferschmidt
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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SchemaUtils


case class DeltaTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    database: String,
    table: String,
    location: Option[Path] = None,
    options: Map[String,String] = Map(),
    properties: Map[String, String] = Map()
) extends BaseRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[DeltaTableRelation])

    private lazy val tableIdentifier: TableIdentifier = {
        TableIdentifier(table, Some(database))
    }

    /**
     * Returns the list of all resources which will be created by this relation.
     *
     * @return
     */
    override def provides: Set[ResourceIdentifier] = {
        Set(ResourceIdentifier.ofHiveTable(table, Some(database)))
    }

    /**
     * Returns the list of all resources which will be required by this relation for creation.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = {
        Set(ResourceIdentifier.ofHiveDatabase(database))
    }

    /**
     * Returns the list of all resources which will are managed by this relation for reading or writing a specific
     * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
     * partition is empty)
     *
     * @param partitions
     * @return
     */
    override def resources(partition: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partition)

        val allPartitions = PartitionSchema(this.partitions).interpolate(partition)
        allPartitions.map(p =>ResourceIdentifier.ofHivePartition(table, Some(database), p.toMap)).toSet
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param schema     - the schema to read. If none is specified, all available columns will be read
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, schema: Option[StructType], partitions: Map[String, FieldValue]): DataFrame = {
        logger.info(s"Reading Delta relation '$identifier' from table $tableIdentifier using partition values $partitions")

        val tableDf = execution.spark.read
            .options(options)
            .table(tableIdentifier.quotedString)
        val df = filterPartition(tableDf, partitions)

        SchemaUtils.applySchema(df, schema)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df        - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        requireAllPartitionKeys(partition)

        // TODO: Static partitions / dynamic partitions
        val partitionSpec = PartitionSchema(partitions).spec(partition)

        logger.info(s"Writing Delta relation '$identifier' to table $tableIdentifier partition ${HiveDialect.expr.partition(partitionSpec)} with mode '$mode'")

        val extDf = SchemaUtils.applySchema(addPartition(df, partition), outputSchema(execution))

        val writer =
            if (partition.nonEmpty) {
                extDf.write
                    .option("replaceWhere", partitionSpec.predicate)
            }
            else {
                extDf.write
            }

        writer
            .format("delta")
            .options(options)
            .mode(mode.batchMode)
            .insertInto(tableIdentifier.quotedString)

        execution.catalog.refreshTable(tableIdentifier)
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
        execution.catalog.tableExists(tableIdentifier)
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
        if (!catalog.tableExists(tableIdentifier)) {
            false
        }
        else if (partitions.nonEmpty) {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            DeltaUtils.isLoaded(execution, tableIdentifier, partitionSpec)
        }
        else {
            val location = catalog.getTableLocation(tableIdentifier)
            DeltaUtils.isLoaded(execution, location)
        }
    }

    /**
     * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
     * relation will not contain any data, but all metadata will be processed
     *
     * @param execution
     */
    override def create(execution: Execution, ifNotExists: Boolean): Unit = {
        val tableExists = exists(execution) == Yes
        if (!ifNotExists || !tableExists) {
            val sparkSchema = HiveTableRelation.cleanupSchema(StructType(fields.map(_.catalogField)))
            logger.info(s"Creating Delta table relation '$identifier' with table $tableIdentifier and schema\n ${sparkSchema.treeString}")

            if (tableExists)
                throw new TableAlreadyExistsException(database, table)

            DeltaUtils.createTable(
                execution,
                Some(tableIdentifier),
                location,
                sparkSchema,
                partitions,
                properties,
                description
            )
        }
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
            val deltaTable = DeltaTable.forName(execution.spark, tableIdentifier.quotedString)
            PartitionSchema(this.partitions).interpolate(partitions).foreach { p =>
                deltaTable.delete(p.predicate)
            }
            deltaTable.vacuum()
        }
        else {
            logger.info(s"Truncating Delta table relation '$identifier' by truncating table $tableIdentifier")
            val deltaTable = DeltaTable.forName(execution.spark, tableIdentifier.quotedString)
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
    override def destroy(execution: Execution, ifExists: Boolean): Unit = {
        require(execution != null)

        val catalog = execution.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            logger.info(s"Destroying Delta table relation '$identifier' by dropping table $tableIdentifier")
            catalog.dropTable(tableIdentifier)
        }
    }

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution, migrationPolicy: MigrationPolicy, migrationStrategy: MigrationStrategy): Unit = {
        ???
    }

    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        Some(DeltaTable.forName(execution.spark, tableIdentifier.quotedString).toDF.schema)
    }
}



@RelationType(kind="deltaTable")
class DeltaTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value = "database", required = false) private var database: String = "default"
    @JsonProperty(value = "table", required = true) private var table: String = ""
    @JsonProperty(value = "location", required = false) private var location: Option[String] = None
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()

    override def instantiate(context: Context): DeltaTableRelation = {
        DeltaTableRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.evaluate(database),
            context.evaluate(table),
            context.evaluate(location).map(p => new Path(p)),
            context.evaluate(options),
            context.evaluate(properties)
        )
    }
}
