/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.exec.ActionCommand


sealed class PhaseCommand(phase:Phase) extends ActionCommand {
    private val logger = LoggerFactory.getLogger(this.getClass)

    @Argument(index=0, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        val args = splitSettings(this.args).toMap
        val job = "main"
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(_:NoSuchJobException) =>
                logger.error(s"Cannot find job '$job'")
                false
            case Failure(_) =>
                logger.error(s"Error instantiating job '$job'")
                false
            case Success(batch) =>
                executeJob(executor, batch, args)
        }
    }

    private def executeJob(executor:Executor, job:Job, args:Map[String,String]) : Boolean = {
        val lifecycle =
            if (noLifecycle)
                Seq(phase)
            else
                Lifecycle.ofPhase(phase)

        val jobDescription = job.description.map("(" + _ + ")").getOrElse("")
        val jobArgs = args.map(kv => kv._1 + "=" + kv._2).mkString(", ")
        logger.info(s"Executing job '${job.name}' $jobDescription with args $jobArgs")

        val runner = executor.session.runner
        val result = runner.executeJob(executor, job, lifecycle, args, force)
        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}

class CreateCommand extends PhaseCommand(Phase.CREATE)

class MigrateCommand extends PhaseCommand(Phase.MIGRATE)

class BuildCommand extends PhaseCommand(Phase.BUILD)

class VerifyCommand extends PhaseCommand(Phase.VERIFY)

class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)

class DestroyCommand extends PhaseCommand(Phase.DESTROY)
