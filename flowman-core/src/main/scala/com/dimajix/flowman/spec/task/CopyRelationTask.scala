/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class CopyRelationTask(
    instanceProperties:Task.Properties,
    source:RelationIdentifier,
    sourcePartitions:Map[String,FieldValue],
    target:RelationIdentifier,
    targetPartition:Map[String,SingleValue],
    parallelism:Int,
    columns:Map[String,String],
    mode:String
) extends BaseTask  {
    private val logger = LoggerFactory.getLogger(classOf[CopyRelationTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Copying from relation '$source' to relation '$target' with partitions ${sourcePartitions.map(kv => kv._1 + "=" + kv._2).mkString(",")}")

        val input = context.getRelation(source)
        val output = context.getRelation(target)
        val schema = if (columns.nonEmpty) Some(SchemaUtils.createSchema(columns.toSeq)) else None
        val data = input.read(executor, schema, sourcePartitions).coalesce(parallelism)
        val xfs = SchemaEnforcer(StructType(output.schema.fields.map(_.sparkField)))
        val conformed = xfs.transform(data)
        output.write(executor, conformed, targetPartition, mode)
        true
    }
}



class CopyRelationTaskSpec extends TaskSpec {
    @JsonProperty(value = "source", required = true) private var source: String = ""
    @JsonProperty(value = "sourcePartitions", required = false) private var sourcePartitions: Map[String, FieldValue] = Map()
    @JsonProperty(value = "target", required = true) private var target: String = ""
    @JsonProperty(value = "targetPartition", required = false) private var targetPartition: Map[String, String] = Map()
    @JsonProperty(value = "columns", required = false) private var columns: Map[String, String] = Map()
    @JsonProperty(value = "parallelism", required = false) private var parallelism: String = "16"
    @JsonProperty(value = "mode", required = false) private var mode: String = "overwrite"


    override def instantiate(context: Context): CopyRelationTask = {
        CopyRelationTask(
            instanceProperties(context),
            RelationIdentifier.parse(context.evaluate(source)),
            sourcePartitions.map{
                case (name,v:SingleValue) => (name,SingleValue(context.evaluate(v.value)))
                case (name,v:ArrayValue) => (name,ArrayValue(v.values.map(context.evaluate)))
                case (name,v:RangeValue) => (name,RangeValue(context.evaluate(v.start), context.evaluate(v.end), v.step.map(context.evaluate)))
            },
            RelationIdentifier.parse(context.evaluate(target)),
            targetPartition.map { case(n,p) => (n,SingleValue(context.evaluate(p))) },
            context.evaluate(parallelism).toInt,
            context.evaluate(columns),
            context.evaluate(mode)
        )
    }
}