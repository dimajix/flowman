/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.types.FieldValue


sealed class PhaseCommand(phase:Phase) extends Command {
    private val logger = LoggerFactory.getLogger(this.getClass)

    @Argument(index=0, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-t", aliases=Array("--target"), usage = "only process specific targets, as specified by a regex", metaVar = "<target>")
    var targets: Array[String] = Array(".*")
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of job with next target in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    var dryRun: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false

    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        val args = splitSettings(this.args).toMap
        val job = "main"
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(e) =>
                logger.error(s"Error instantiating job '$job': ${reasons(e)}")
                false
            case Success(job) =>
                executeJob(session, job, job.parseArguments(args))
        }
    }

    private def executeJob(session: Session, job:Job, args:Map[String,FieldValue]) : Boolean = {
        val lifecycle =
            if (noLifecycle)
                Seq(phase)
            else
                Lifecycle.ofPhase(phase)

        val jobDescription = job.description.map("(" + _ + ")").getOrElse("")
        val jobArgs = args.map(kv => kv._1 + "=" + kv._2).mkString(", ")
        logger.info(s"Executing job '${job.name}' $jobDescription with args $jobArgs")

        job.interpolate(args).forall { args =>
            val runner = session.runner
            val result = runner.executeJob(job, lifecycle, args, targets.map(_.r), force, keepGoing, dryRun)
            result.success
        }
    }
}


class ValidateCommand extends PhaseCommand(Phase.VALIDATE)
class CreateCommand extends PhaseCommand(Phase.CREATE)
class BuildCommand extends PhaseCommand(Phase.BUILD)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
