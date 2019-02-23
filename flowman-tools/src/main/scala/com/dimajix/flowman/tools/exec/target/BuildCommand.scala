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

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.BuildTargetTask
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.tools.exec.ActionCommand


class BuildCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[BuildCommand])

    @Argument(usage = "specifies target(s) to build", metaVar = "<target>")
    var targets: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "builds all targets, even the disabled ones")
    var all: Boolean = false


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Processing outputs {}", if (targets != null) targets.mkString(",") else "all")

        // Then build output operations
        val toRun =
            if (all)
                project.targets.keys.toSeq
            else if (targets.nonEmpty)
                targets.toSeq
            else
                project.targets.filter(_._2.enabled).keys.toSeq

        val task = BuildTargetTask(toRun, s"Building targets ${toRun.mkString(",")}")
        val job = Job(Seq(task), "build-targets", "Build targets")

        val runner = executor.runner
        val result = runner.execute(executor, job, Map(), true)

        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}
