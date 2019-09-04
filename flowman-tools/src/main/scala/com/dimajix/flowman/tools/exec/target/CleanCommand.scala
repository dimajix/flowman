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

package com.dimajix.flowman.tools.exec.target

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.task.CleanTargetTask
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.tools.exec.ActionCommand


class CleanCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[CleanCommand])

    @Argument(usage = "specifies target(s) to clean", metaVar = "<target>")
    var targets: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "cleans all outputs, even the disabled ones")
    var all: Boolean = false


    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        logger.info("Cleaning outputs {}", if (targets != null) targets.mkString(",") else "all")

        val toRun =
            if (all)
                project.targets.keys.toSeq
            else if (targets.nonEmpty)
                targets.toSeq
            else
                project.targets.keys.toSeq
                    .map(t => context.getTarget(TargetIdentifier(t)))
                    .filter(_.enabled)
                    .map(_.name)

        val task = CleanTargetTask(context, toRun.map(TargetIdentifier.parse), s"Cleaning targets ${toRun.mkString(",")}")
        val job = Job.builder(context)
            .setName("clean-targets")
            .setDescription("Clean targets")
            .addTask(task)
            .build()

        val runner = executor.runner
        val result = runner.execute(executor, job, Map(), true)

        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}
