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

package com.dimajix.flowman.spec.target

import java.time.Duration

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.relation.DeltaFileRelation
import com.dimajix.flowman.spec.relation.DeltaTableRelation
import com.dimajix.flowman.spec.relation.RelationReferenceSpec


case class DeltaVacuumTarget(
    override val instanceProperties:Target.Properties,
    relation:Reference[Relation],
    retentionTime:Option[Duration] = None,
    compaction:Boolean = false,
    minFiles:Int = 32,
    maxFiles:Int = 64
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[DeltaVacuumTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases: Set[Phase] = Set(Phase.BUILD)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        val rel = relation.value
        rel.provides ++ rel.requires
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.BUILD => Unknown
            case _ => No
        }
    }

    /**
     * Abstract method which will perform the output operation. This method may throw an exception, which will be
     * caught an wrapped in the final [[TargetResult]].
     *
     * @param execution
     */
    override protected def build(execution: Execution): Unit = {
        val deltaTable = relation.value match {
            case table:DeltaTableRelation => DeltaTable.forName(execution.spark, TableIdentifier(table.table, Some(table.database)).toString())
            case files:DeltaFileRelation => DeltaTable.forPath(execution.spark, files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }

        if (compaction)
            compact(deltaTable)

        logger.info(s"Vacuum Delta relation '${relation.identifier}'")
        retentionTime match {
            case Some(duration) => deltaTable.vacuum(duration.toHours)
            case None => deltaTable.vacuum()
        }
    }

    private def compact(deltaTable:DeltaTable) : Unit = {
        val spark = deltaTable.toDF.sparkSession
        val deltaLog = relation.value match {
            case table:DeltaTableRelation => DeltaLog.forTable(spark, TableIdentifier(table.table, Some(table.database)))
            case files:DeltaFileRelation => DeltaLog.forTable(spark, files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }

        val partitionColumns = deltaLog.snapshot.metadata.partitionColumns
        if (partitionColumns.nonEmpty) {
            compactPartitioned(deltaTable, deltaLog)
        }
        else {
            compactUnpartitioned(deltaTable, deltaLog)
        }
    }

    private def writeTable(df:DataFrame, filter:Option[String]) : Unit = {
        val writer = df.write
            .format("delta")
            .mode("overwrite")
            .option("dataChange", "false")
        filter.map(writer.option("replaceWhere", _))

        relation.value match {
            case table:DeltaTableRelation => writer.insertInto(TableIdentifier(table.table, Some(table.database)).toString())
            case files:DeltaFileRelation => writer.save(files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }
    }

    private def compactUnpartitioned(deltaTable:DeltaTable, deltaLog:DeltaLog) : Unit = {
        val currentFileCount = deltaLog
            .snapshot
            .allFiles
            .toDF
            .count()
        if (currentFileCount > maxFiles) {
            logger.info(s"Compacting Delta relation '${relation.identifier}'")
            val df = deltaTable.toDF
                .repartition(minFiles)
            writeTable(df, None)
        }
    }

    private def compactPartitioned(deltaTable:DeltaTable, deltaLog:DeltaLog) : Unit = {
        val partitionColumns = deltaLog.snapshot.metadata.partitionColumns
        val compactionPartitionList = deltaLog
            .snapshot
            .allFiles
            .toDF
            .groupBy(partitionColumns.map(p => col("partitionValues." + p)): _*)
            .agg(
                functions.count("*").as("count"),
                functions.max("modificationTime").as("modificationTime")
            )
            .where(col("count") > maxFiles)
            //.where(s"`max(modificationTime)` <= ${compactionThresholdEpochDay}")
            .select(partitionColumns.map(col): _*)
            .sort(partitionColumns.map(col): _*)
            .collect

        ThreadUtils.parmap(compactionPartitionList, "delta-vacuum-", 8) { partition =>
            val partitionFilter = partition.toSeq.zip(partitionColumns)
                    .map { case (v, k) => col(k) === lit(v) }
                    .reduce(_ && _)

            val partitionString = partition.toSeq.zip(partitionColumns).map(kv => kv._2 + "=" + kv._1.toString).mkString(",")
            logger.info(s"Compacting Delta relation '${relation.identifier}' for partition ${partitionString}")
            val df = deltaTable.toDF
                .where(partitionFilter.expr.sql)
                .repartition(minFiles)
            writeTable(df, Some(partitionFilter.expr.sql))
        }
    }
}


@TargetType(kind="deltaVacuum")
class DeltaVacuumTargetSpec extends TargetSpec {
    @JsonProperty(value = "relation", required = true) private var relation: RelationReferenceSpec = _
    @JsonProperty(value = "retentionTime", required = true) private var retentionTime: Option[String] = None
    @JsonProperty(value = "compaction", required = false) private var compaction: String = "false"
    @JsonProperty(value = "minFiles", required = false) private var minFiles: String = "16"
    @JsonProperty(value = "maxFiles", required = false) private var maxFiles: String = "64"

    override def instantiate(context: Context): DeltaVacuumTarget = {
        DeltaVacuumTarget(
            instanceProperties(context),
            relation.instantiate(context),
            context.evaluate(retentionTime).map(Duration.parse),
            context.evaluate(compaction).toBoolean,
            context.evaluate(minFiles).toInt,
            context.evaluate(maxFiles).toInt
        )
    }
}
