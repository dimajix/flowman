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

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.DeltaTableIdentifier
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.commands.CreateDeltaTableCommand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.model.PartitionField


object DeltaUtils {
    def tableIdentifier(location:Path) : TableIdentifier = {
        TableIdentifier(location.toString, Some("delta"))
    }

    def getLocation(execution: Execution, tableIdentifier:TableIdentifier) : Path = {
        val sparkSession = execution.spark
        if (DeltaTableUtils.isDeltaTable(sparkSession, tableIdentifier)) {
            val tbl = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
            new Path(tbl.location)
        } else {
            throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(table = Some(tableIdentifier)))
        }
    }

    /**
     * Returns true if the specified location contains a non-empty Delta table
     * @param execution
     * @param location
     * @return
     */
    def isLoaded(execution: Execution, location:Path) : Boolean = {
        if (DeltaTable.isDeltaTable(execution.spark, location.toString)) {
            val deltaLog = DeltaLog.forTable(execution.spark, location)
            val snapshot = deltaLog.snapshot
            DeltaLog.filterFileList(
                snapshot.metadata.partitionSchema,
                snapshot.allFiles.toDF(),
                Seq()).count() > 0
        }
        else {
            false
        }
    }

    /**
     * Returns true if the specified location contains a Delta table with the given partiton being non-empty
     * @param execution
     * @param location
     * @return
     */
    def isLoaded(execution: Execution, location:Path, partition:PartitionSpec) : Boolean = {
        val deltaLog = DeltaLog.forTable(execution.spark, location)
        DeltaUtils.isLoaded(deltaLog, partition)
    }

    /**
     * Returns true if the specified location contains a Delta table with the given partiton being non-empty
     * @param execution
     * @param location
     * @return
     */
    def isLoaded(execution: Execution, table:TableIdentifier, partition:PartitionSpec) : Boolean = {
        val deltaLog = DeltaLog.forTable(execution.spark, table)
        DeltaUtils.isLoaded(deltaLog, partition)
    }

    /**
     * Returns true if the partition of the given Delta table is non-empty
     * @param execution
     * @param location
     * @return
     */
    def isLoaded(deltaLog:DeltaLog, partition:PartitionSpec) : Boolean = {
        val partitionFilters = partition
            .values
            .map { case (k, v) => (col(k) === lit(v)).expr }
            .toSeq

        val snapshot = deltaLog.snapshot
        DeltaLog.filterFileList(
            snapshot.metadata.partitionSchema,
            snapshot.allFiles.toDF(),
            partitionFilters).count() > 0
    }

    def upsert(table:DeltaTable, df: DataFrame, keyColumns:Iterable[String], partitionSpec: PartitionSpec) : Unit = {
        if (keyColumns.isEmpty)
            throw new IllegalArgumentException(s"Cannot perform upsert activity without primary key")

        val keyCondition = keyColumns.map(k => col("relation." + k) <=> col("df." + k))
        val partitionCondition = partitionSpec.values.map { case (k, v) => (col("relation." + k) === lit(v)) }
        val mergeCondition = (keyCondition ++ partitionCondition).reduce(_ && _)
        table.as("relation")
            .merge(df.as("df"), mergeCondition)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
    }

    def merge(table:DeltaTable, df: DataFrame, mergeCondition:Column, clauses:Seq[MergeClause]) : Unit = {
        val builder = table.as("target")
            .merge(df.as("source"), mergeCondition)
        clauses.foldLeft(builder) { (mergeBuilder, clause) =>
                clause match {
                    case InsertClause(condition, columns) =>
                        val b2 = condition.map(mergeBuilder.whenNotMatched).getOrElse(mergeBuilder.whenNotMatched())
                        if (columns.nonEmpty)
                            b2.insert(columns)
                        else
                            b2.insertAll()
                    case UpdateClause(condition, columns) =>
                        val b2 = condition.map(mergeBuilder.whenMatched).getOrElse(mergeBuilder.whenMatched())
                        if (columns.nonEmpty)
                            b2.update(columns)
                        else
                            b2.updateAll()
                    case DeleteClause(condition) =>
                        val b2 = condition.map(mergeBuilder.whenMatched).getOrElse(mergeBuilder.whenMatched())
                        b2.delete()
                }

            }
            .execute()
    }

    def createTable(execution: Execution, table:Option[TableIdentifier], location:Option[Path], schema:StructType, partitions: Seq[PartitionField], properties:Map[String,String], description:Option[String]): Unit = {
        val tableIdentifier = table.getOrElse(this.tableIdentifier(location.get))

        // Configure catalog table by assembling all options
        val tableByPath = table.isEmpty
        val catalogTable = CatalogTable(
            identifier = tableIdentifier,
            tableType = if (location.nonEmpty) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
            storage = CatalogStorageFormat(
                locationUri = location.map(_.toUri),
                inputFormat = None,
                outputFormat = None,
                serde = None,
                compressed = false,
                properties = Map()
            ),
            provider = Some("delta"),
            schema = schema,
            partitionColumnNames = partitions.map(_.name),
            properties = properties,
            comment = description
        )

        val cmd = CreateDeltaTableCommand(
            catalogTable,
            None,
            SaveMode.ErrorIfExists,
            None,
            tableByPath = tableByPath
        )
        cmd.run(execution.spark)

        // TODO: Inform catalog about new table
        //catalog.createTable(catalogTable, false)
    }

    def createLog(spark:SparkSession, table:CatalogTable) : Unit = {
        val isManagedTable = table.tableType == CatalogTableType.MANAGED
        val deltaLog = DeltaLog.forTable(spark, new Path(table.location))
        val newMetadata = Metadata(
            description = table.comment.orNull,
            schemaString = table.schema.json,
            partitionColumns = table.partitionColumnNames,
            configuration = table.properties
        )
        val txn = deltaLog.startTransaction()
        txn.updateMetadataForNewTable(newMetadata)
        val op = DeltaOperations.ReplaceTable(newMetadata, isManagedTable, orCreate = false, false)
        txn.commit(Nil, op)
    }
}
