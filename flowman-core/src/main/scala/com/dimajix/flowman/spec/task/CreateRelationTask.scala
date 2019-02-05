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

object CreateRelationTask {
    def apply(relations:Seq[String]) : CreateRelationTask = {
        val task = new CreateRelationTask
        task._relations = relations
        task
    }
}


class CreateRelationTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CreateRelationTask])

    @JsonProperty(value="relation", required=true) private var _relations:Seq[String] = Seq()

    def relations(implicit context: Context) : Seq[RelationIdentifier] = _relations.map(i => RelationIdentifier.parse(context.evaluate(i)))

    /**
      * Instantiates all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        relations.foreach(o => createRelation(executor, o))
        true
    }

    private def createRelation(executor: Executor, identifier:RelationIdentifier) : Boolean = {
        implicit val context = executor.context
        val relation = context.getRelation(identifier)

        logger.info("Creating relation '{}'", identifier.toString)
        relation.create(executor)
        true
    }
}

