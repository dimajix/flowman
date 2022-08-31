/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.types.FieldValue


sealed class PhaseCommand(phase:Phase) extends Command {
    private val logger = LoggerFactory.getLogger(getClass)

    @Argument(index=0, required=true, usage = "specifies job to run", metaVar = "<job>")
    var job: String = ""
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-t", aliases=Array("--target"), usage = "only process specific targets, as specified by a regex", metaVar = "<target>")
    var targets: Array[String] = Array(".*")
    @Option(name = "-d", aliases=Array("--dirty"), usage = "mark targets as being dirty, as specified by a regex", metaVar = "<target>")
    var dirtyTargets: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of job with next target in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    var dryRun: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false
    @Option(name = "-j", aliases=Array("--jobs"), usage = "number of jobs to run in parallel")
    var parallelism: Int = 1

    override def execute(session: Session, project: Project, context:Context) : Status = {
        val args = splitSettings(this.args).toMap
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(e) =>
                logger.error(s"Error instantiating job '$job':\n  ${reasons(e)}")
                Status.FAILED
            case Success(job) =>
                executeJob(session, job, job.parseArguments(args))
        }
    }

    private def executeJob(session: Session, job:Job, args:Map[String,FieldValue]) : Status = {
        val lifecycle =
            if (noLifecycle)
                Seq(phase)
            else
                Lifecycle.ofPhase(phase)

        if (parallelism > 1)
            executeParallel(session, job, args, lifecycle)
        else
            executeLinear(session, job, args, lifecycle)
    }

    private def executeLinear(session: Session, job:Job, args:Map[String,FieldValue], lifecycle: Seq[Phase]) : Status = {
        Status.ofAll(job.interpolate(args), keepGoing=keepGoing) { args =>
            val runner = session.runner
            runner.executeJob(job, lifecycle, args, targets.map(_.r), dirtyTargets=dirtyTargets.map(_.r), force=force, keepGoing=keepGoing, dryRun=dryRun)
        }
    }

    private def executeParallel(session: Session, job:Job, args:Map[String,FieldValue], lifecycle: Seq[Phase]) : Status = {
        Status.parallelOfAll(job.interpolate(args).toSeq, parallelism, keepGoing=keepGoing, prefix="JobExecution") { args =>
            val runner = session.runner
            runner.executeJob(job, lifecycle, args, targets.map(_.r), dirtyTargets=dirtyTargets.map(_.r), force=force, keepGoing=keepGoing, dryRun=dryRun)
        }
    }
}

class ValidateCommand extends PhaseCommand(Phase.VALIDATE)
class CreateCommand extends PhaseCommand(Phase.CREATE)
class BuildCommand extends PhaseCommand(Phase.BUILD)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
