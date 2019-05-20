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
import com.dimajix.flowman.spec.model.Relation


object ShowRelationTask {
    def apply(relation:Relation, columns:Seq[String], limit:Int) : ShowRelationTask = {
        ShowRelationTask(
            Task.Properties(null),
            relation,
            columns,
            limit
        )
    }
}

case class ShowRelationTask(
    instanceProperties:Task.Properties,
    relation:Relation,
    columns:Seq[String],
    limit:Int
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[ShowRelationTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Showing first $limit rows of relation '${relation.identifier}'")

        val table = relation.read(executor, null)
        val projection = if (columns.nonEmpty)
            table.select(columns.map(c => table(c)):_*)
        else
            table

        projection.show(limit, truncate = false)
        true
    }
}




class ShowRelationTaskSpec extends TaskSpec {
    @JsonProperty(value = "relation", required = true) private var relation: String = _
    @JsonProperty(value = "limit", required = true) private var limit: String = "100"
    @JsonProperty(value = "columns", required = true) private var columns: Seq[String] = Seq()

    override def instantiate(context: Context): Task = {
        ShowRelationTask(
            instanceProperties(context),
            context.getRelation(RelationIdentifier(context.evaluate(relation))),
            columns.map(context.evaluate),
            context.evaluate(limit).toInt
        )
    }
}