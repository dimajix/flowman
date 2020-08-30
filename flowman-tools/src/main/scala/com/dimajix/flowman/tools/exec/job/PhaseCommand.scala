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

package com.dimajix.flowman.tools.exec.job

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

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
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.types.FieldValue


sealed class PhaseCommand(phase:Phase) extends ActionCommand {
    private val logger = LoggerFactory.getLogger(getClass)

    @Argument(index=0, required=true, usage = "specifies job to run", metaVar = "<job>")
    var job: String = ""
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of job with next target in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        val args = splitSettings(this.args).toMap
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(NonFatal(e)) =>
                logger.error(s"Error instantiating job '$job': ${e.getMessage()}")
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

        job.interpolate(args).forall { args =>
            val runner = session.runner
            val result = runner.executeJob(job, lifecycle, args, force, keepGoing)
            result match {
                case Status.SUCCESS => true
                case Status.SKIPPED => true
                case _ => false
            }
        }
    }
}

class CreateCommand extends PhaseCommand(Phase.CREATE)
class BuildCommand extends PhaseCommand(Phase.BUILD)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
