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

package com.dimajix.flowman.tools.exec.job

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.tools.exec.ActionCommand


class RunCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(index=0, required=true, usage = "specifies job to run", metaVar = "<job>")
    var job: String = ""
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "runs all outputs, even the disabled ones")
    var all: Boolean = false
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false


    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        val args = splitSettings(this.args).toMap
        Try {
            project.jobs(job)
        }
        match {
            case Failure(_) =>
                logger.error(s"Cannot find job $job")
                false
            case Success(job) =>
                executeJob(executor, job.instantiate(context), args)
        }
    }

    private def executeJob(executor:Executor, job:Job, args:Map[String,String]) : Boolean = {
        val jobDescription = job.description.map("(" + _ + ")").getOrElse("")
        val jobArgs = args.map(kv => kv._1 + "=" + kv._2).mkString(", ")
        logger.info(s"Executing job '${job.name}' $jobDescription with args $jobArgs")

        val runner = executor.runner
        val result = runner.execute(executor, job, args, force)
        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}
