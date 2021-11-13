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

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableAddColumnsDeltaCommand
import org.apache.spark.sql.delta.commands.AlterTableChangeColumnDeltaCommand
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.spark.sql.delta.QualifiedColumn
import com.dimajix.spark.sql.SchemaUtils


abstract class DeltaRelation(options: Map[String,String]) extends BaseRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[DeltaRelation])

    protected def loadDeltaTable(execution: Execution) : DeltaTableV2

    /**
     * Reads data from a streaming source
     *
     * @param execution
     * @param schema
     * @return
     */
    protected def readStreamFrom(execution: Execution, location: Path, schema: Option[StructType]): DataFrame = {
        val df = streamReader(execution, "delta", options).load(location.toString)
        SchemaUtils.applySchema(df, schema)
    }

    /**
     * Writes data to a streaming sink
     *
     * @param execution
     * @param df
     * @return
     */
    protected def writeStreamTo(execution: Execution, df: DataFrame, location: Path, mode: OutputMode, trigger: Trigger, checkpointLocation: Path): StreamingQuery = {
        val writer = streamWriter(execution, df, "delta", options, mode.streamMode, trigger, checkpointLocation)
        if (partitions.nonEmpty) {
            writer
                .partitionBy(partitions.map(_.name):_*)
                .start(location.toString)
        }
        else {
            writer.start(location.toString)
        }
    }

    /**
     * Performs actual migration
     * @param execution
     * @param migrationPolicy
     * @param migrationStrategy
     */
    protected def migrateInternal(execution: Execution, migrationPolicy: MigrationPolicy, migrationStrategy: MigrationStrategy): Unit = {
        val table = loadDeltaTable(execution)
        val sourceSchema = com.dimajix.flowman.types.StructType.of(table.schema())
        val targetSchema = com.dimajix.flowman.types.SchemaUtils.replaceCharVarchar(fullSchema.get)

        val requiresMigration = TableChange.requiresMigration(sourceSchema, targetSchema, migrationPolicy)

        if (requiresMigration) {
            doMigration(execution, table, sourceSchema, targetSchema, migrationPolicy, migrationStrategy)
        }
    }
    private def doMigration(execution: Execution, table:DeltaTableV2, currentSchema:com.dimajix.flowman.types.StructType, targetSchema:com.dimajix.flowman.types.StructType, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for Delta relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate Delta relation '$identifier', since migrations are disabled.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER =>
                val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
                if (migrations.exists(m => !supported(m))) {
                    logger.error(s"Cannot migrate Delta relation '$identifier', since that would require unsupported changes.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
                    throw new MigrationFailedException(identifier)
                }
                alter(migrations)
            case MigrationStrategy.ALTER_REPLACE =>
                val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
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
            logger.info(s"Migrating Delta relation '$identifier'. New schema:\n${targetSchema.treeString}")

            try {
                val spark = execution.spark
                migrations.foreach {
                    case AddColumn(column) =>
                        val table = loadDeltaTable(execution)
                        AlterTableAddColumnsDeltaCommand(
                            table,
                            Seq(QualifiedColumn(column.name, column.catalogType, column.nullable, column.description)
                            )).run(spark)
                    case UpdateColumnNullability(column, nullable) =>
                        val table = loadDeltaTable(execution)
                        val oldColumn = table.schema()(column)
                        AlterTableChangeColumnDeltaCommand(
                            table,
                            Seq(),
                            column,
                            oldColumn.copy(nullable=nullable),
                            None
                        ).run(spark)
                    case UpdateColumnType(column, dataType) =>
                        val table = loadDeltaTable(execution)
                        val oldColumn = table.schema()(column)
                        AlterTableChangeColumnDeltaCommand(
                            table,
                            Seq(),
                            column,
                            oldColumn.copy(dataType=dataType.catalogType),
                            None
                        ).run(spark)
                    case UpdateColumnComment(column, comment) =>
                        val table = loadDeltaTable(execution)
                        val oldColumn = table.schema()(column)
                        val field = comment.map(c => oldColumn.withComment(c))
                            .getOrElse(oldColumn.copy(metadata = new MetadataBuilder().withMetadata(oldColumn.metadata).remove("comment").build()))
                        AlterTableChangeColumnDeltaCommand(
                            table,
                            Seq(),
                            column,
                            field,
                            None
                        ).run(spark)
                }
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def recreate() : Unit = {
            logger.info(s"Migrating Delta relation '$identifier' by dropping/recreating.")
            try {
                destroy(execution, true)
                create(execution, true)
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
                case x:TableChange => throw new UnsupportedOperationException(s"Table change ${x} not supported")
            }
        }
    }

    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        Some(loadDeltaTable(execution).schema())
    }
}
