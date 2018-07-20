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

package com.dimajix.flowman.tools.exec.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    var tablename: String = ""

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Describing relation '$tablename'")

        Try {
            val relation = context.getRelation(RelationIdentifier.parse(tablename))
            val schema = relation.schema
            if (schema == null)
                logger.error(s"Relation '$tablename' does not provide an explicit schema")
            else
                schema.printTree
        } match {
            case Success(_) =>
                logger.info("Successfully finished describing relation")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while describing relation '$tablename':", e)
                false
        }
    }
}
