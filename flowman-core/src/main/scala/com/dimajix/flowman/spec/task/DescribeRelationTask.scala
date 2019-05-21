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


object DescribeRelationTask {
    def apply(context: Context, relation:RelationIdentifier) : DescribeRelationTask = {
        DescribeRelationTask(
            Task.Properties(context),
            relation
        )
    }
}


case class DescribeRelationTask(
    instanceProperties:Task.Properties,
    relation:RelationIdentifier
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DescribeRelationTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Describing relation '$relation'")

        val rel = context.getRelation(relation)
        val schema = rel.schema
        if (schema == null)
            logger.error(s"Relation '$relation' does not provide an explicit schema")
        else
            schema.printTree
        true
    }
}



class DescribeRelationTaskSpec extends TaskSpec {
    @JsonProperty(value = "relation", required = true) private var relation: String = _

    override def instantiate(context: Context): Task = {
        DescribeRelationTask(
            instanceProperties(context),
            RelationIdentifier(context.evaluate(relation))
        )
    }
}
