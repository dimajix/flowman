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
import com.dimajix.flowman.spec.MappingOutputIdentifier


object ShowMappingTask {
    def apply(context: Context, mappings:Seq[MappingOutputIdentifier], columns:Seq[String], limit:Int) : ShowMappingTask = {
        ShowMappingTask(
            Task.Properties(context),
            mappings,
            columns,
            limit
        )
    }
}

case class ShowMappingTask(
    instanceProperties:Task.Properties,
    mappings:Seq[MappingOutputIdentifier],
    columns:Seq[String],
    limit:Int
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[ShowMappingTask])

    override def execute(executor:Executor) : Boolean = {
        mappings.foreach(m => showMapping(executor, m))
        true
    }

    private def showMapping(executor:Executor, mapping:MappingOutputIdentifier) : Boolean = {
        logger.info(s"Showing first $limit rows of mapping '$mapping'")

        val instance = context.getMapping(mapping.mapping)
        val table = executor.instantiate(instance, mapping.output)
        val projection = if (columns.nonEmpty)
            table.select(columns.map(c => table(c)):_*)
        else
            table

        projection.show(limit, truncate = false)
        true
    }
}



class ShowMappingTaskSpec extends TaskSpec {
    @JsonProperty(value = "mapping", required = true) private var mapping: Seq[String] = Seq()
    @JsonProperty(value = "limit", required = false) private var limit: String = "100"
    @JsonProperty(value = "columns", required = false) private var columns: Seq[String] = Seq()


    override def instantiate(context: Context): ShowMappingTask = {
        ShowMappingTask(
            instanceProperties(context),
            mapping.map(r => MappingOutputIdentifier(context.evaluate(r))),
            columns.map(context.evaluate),
            context.evaluate(limit).toInt
        )
    }
}
