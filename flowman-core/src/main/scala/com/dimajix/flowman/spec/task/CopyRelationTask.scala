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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class CopyRelationTask extends BaseTask  {
    private val logger = LoggerFactory.getLogger(classOf[CopyRelationTask])

    @JsonProperty(value="source", required=true) private var _source:String = ""
    @JsonProperty(value="sourcePartitions", required=false) private var _sourcePartitions:Map[String,FieldValue] = Map()
    @JsonProperty(value="target", required=true) private var _target:String = ""
    @JsonProperty(value="targetPartition", required=false) private var _targetPartition:Map[String,String] = Map()
    @JsonProperty(value="columns", required=false) private var _columns:Map[String,String] = _
    @JsonProperty(value="parallelism", required=false) private var _parallelism:String = "16"
    @JsonProperty(value="mode", required=false) private var _mode:String = "overwrite"

    def source(implicit context:Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_source))
    def sourcePartitions(implicit context:Context) : Map[String,FieldValue] = {
        _sourcePartitions.mapValues {
            case v: SingleValue => SingleValue(context.evaluate(v.value))
            case v: ArrayValue => ArrayValue(v.values.map(context.evaluate))
            case v: RangeValue => RangeValue(context.evaluate(v.start), context.evaluate(v.end), context.evaluate(v.step))
        }
    }
    def target(implicit context:Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_target))
    def targetPartition(implicit context: Context) : Map[String,String] = _targetPartition.mapValues(context.evaluate)
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt
    def columns(implicit context:Context) : Map[String,String] = if (_columns != null) _columns.mapValues(context.evaluate) else null
    def mode(implicit context:Context) : String = context.evaluate(_mode)

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val source = this.source
        val sourceRelation = context.getRelation(source)
        val sourcePartitions = this.sourcePartitions
        val target = this.target
        val targetRelation = context.getRelation(target)
        val targetPartition = this.targetPartition.mapValues(v => SingleValue(v))
        logger.info(s"Copying from relation '$source' to relation '$target' with partitions ${sourcePartitions.map(kv => kv._1 + "=" + kv._2).mkString(",")}")

        val fields = this.columns
        val schema = if (fields != null && fields.nonEmpty) SchemaUtils.createSchema(fields.toSeq) else null
        val data = sourceRelation.read(executor, schema, sourcePartitions).coalesce(parallelism)
        targetRelation.write(executor, data, targetPartition, mode)
        true
    }
}
