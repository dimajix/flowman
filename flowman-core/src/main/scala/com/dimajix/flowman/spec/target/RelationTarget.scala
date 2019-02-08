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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.SingleValue


class RelationTarget extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[RelationTarget])

    @JsonProperty(value="relation", required=true) private var _relation:String = _
    @JsonProperty(value="mode", required=false) private var _mode:String = "overwrite"
    @JsonProperty(value="partition", required=false) private var _partition:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var _parallelism:String = "16"
    @JsonProperty(value="rebalance", required=false) private var _rebalance:String = "false"

    def relation(implicit context: Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_relation))
    def mode(implicit context: Context) : String = context.evaluate(_mode)
    def partition(implicit context: Context) : Map[String,String] = _partition.mapValues(context.evaluate)
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt
    def rebalance(implicit context: Context) : Boolean = context.evaluate(_rebalance).toBoolean

    override def build(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : Unit = {
        implicit var context = executor.context
        val partition = this.partition.mapValues(v => SingleValue(v))
        val rebalance = this.rebalance
        val target = this.relation
        val input = this.input

        logger.info(s"Writing mapping '$input' to relation '$target' into partition $partition")
        val relation = context.getRelation(target)
        val table = if (rebalance)
            tables(input).repartition(parallelism)
        else
            tables(input).coalesce(parallelism)

        relation.write(executor, table, partition, mode)
    }

    override def clean(executor: Executor): Unit = {
        implicit var context = executor.context
        val partition = this.partition.mapValues(v => SingleValue(v))
        val target = this.relation

        logger.info(s"Cleaning partition $partition of relation '$target'")
        val relation = context.getRelation(target)
        relation.clean(executor, partition)
    }
}
