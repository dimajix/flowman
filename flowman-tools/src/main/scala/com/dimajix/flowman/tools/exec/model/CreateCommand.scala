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

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.task.CreateRelationTask
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.tools.exec.target.BuildCommand


class CreateCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[BuildCommand])

    @Argument(usage = "specifies relations to create", metaVar = "<relation>")
    var relations: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-i", aliases=Array("--ignoreIfExists"), usage = "does not do anything if relation already exists")
    var ignoreIfExists: Boolean = false

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        logger.info("Creating relations {}", if (relations != null) relations.mkString(",") else "all")

        val toRun =
            if (relations.nonEmpty)
                relations.toSeq
            else
                project.relations.keys.toSeq

        val task = CreateRelationTask(context, toRun.map(RelationIdentifier.parse), ignoreIfExists)
        val job = Job.builder(context)
            .setName("create-relations")
            .setDescription("Create relations")
            .addTask(task)
            .build()

        val runner = executor.runner
        val result = runner.executeBatch(executor, job)

        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}
