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

package com.dimajix.flowman.tools.exec.project

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.JobStatus
import com.dimajix.flowman.tools.exec.ActionCommand


class RunCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        project.main.forall( name => {
            Try {
                project.jobs(name)
            }
            match {
                case Failure(e) =>
                    logger.error(s"Cannot find job $name")
                    false
                case Success(job) =>
                    executeJob(executor, job)
            }
        })
    }

    private def executeJob(executor:Executor, job:Job) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Executing job '${job.name}' (${job.description})")
        val runner = context.runner
        val result = runner.execute(executor, job, Map(), force)
        result match {
            case JobStatus.SUCCESS => true
            case JobStatus.SKIPPED => true
            case _ => false
        }
    }
}
