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
import com.dimajix.flowman.spec.MappingIdentifier


object ShowMappingTask {
    def apply(mapping:String, columns:Seq[String], limit:Int) : ShowMappingTask = {
        val task = new ShowMappingTask
        task._mapping = mapping
        task._columns = columns
        task._limit = limit.toString
        task
    }
}


class ShowMappingTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[ShowMappingTask])

    @JsonProperty(value="mapping", required=true) private var _mapping:String = _
    @JsonProperty(value="limit", required=false) private[spec] var _limit:String = "100"
    @JsonProperty(value="columns", required=false) private[spec] var _columns:Seq[String] = Seq()

    def mapping(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_mapping))
    def limit(implicit context: Context) : Int = context.evaluate(_limit).toInt
    def columns(implicit context: Context) : Seq[String] = if (_columns != null) _columns.map(context.evaluate) else null

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val limit = this.limit
        val identifier = this.mapping
        val columns = this.columns
        logger.info(s"Showing first $limit rows of mapping '$identifier'")

        val table = executor.instantiate(identifier)
        val projection = if (columns.nonEmpty)
            table.select(columns.map(c => table(c)):_*)
        else
            table

        projection.show(limit, truncate = false)
        true
    }
}
