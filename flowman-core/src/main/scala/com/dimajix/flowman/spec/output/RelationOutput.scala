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

package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.schema.SingleValue


class RelationOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[RelationOutput])

    @JsonProperty(value="target", required=true) private var _target:String = _
    @JsonProperty(value="mode", required=false) private var _mode:String = "overwrite"
    @JsonProperty(value="partition", required=false) private var _partition:Map[String,String] = _
    @JsonProperty(value="parallelism", required=false) private var _parallelism:String = "16"

    def target(implicit context: Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_target))
    def mode(implicit context: Context) : String = context.evaluate(_mode)
    def partition(implicit context: Context) : Map[String,String] = if (_partition != null) _partition.mapValues(context.evaluate) else Map()
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt

    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit = {
        implicit var context = executor.context
        logger.info("Writing to relation '{}'", target)
        val partition = this.partition.mapValues(v => SingleValue(v))
        val relation = context.getRelation(target)
        val table = input(this.input).coalesce(parallelism)
        relation.write(executor, table, partition, mode)
    }
}
