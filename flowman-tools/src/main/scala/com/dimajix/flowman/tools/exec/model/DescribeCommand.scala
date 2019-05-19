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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.DescribeRelationTask
import com.dimajix.flowman.tools.exec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    var relation: String = ""

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        val task = DescribeRelationTask(relation)

        Try {
            task.execute(executor)
        } match {
            case Success(_) =>
                logger.info("Successfully finished describing relation")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while describing relation '$relation':", e)
                false
        }
    }
}
