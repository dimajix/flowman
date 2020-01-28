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
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.tools.exec.ActionCommand


class PhaseCommand(phase:Phase) extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[PhaseCommand])

    @Argument(usage = "specifies target(s) to execute", metaVar = "<target>")
    var targets: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false


    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        logger.info("Cleaning outputs {}", if (targets != null) targets.mkString(",") else "all")

        val toRun =
            if (targets.nonEmpty)
                targets.toSeq
            else
                project.targets.keys.toSeq

        val job = Job.builder(context)
            .setName("cli-execute-targets")
            .setDescription("Execute targets via CLI")
            .setTargets(toRun.map(TargetIdentifier.parse))
            .build()

        val lifecycle =
            if (noLifecycle)
                Seq(phase)
            else
                Lifecycle.ofPhase(phase)

        val runner = session.runner
        val executor = session.executor
        val result = runner.executeJob(executor, job, lifecycle, Map(), force)
        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}


class CreateCommand extends PhaseCommand(Phase.CREATE)
class BuildCommand extends PhaseCommand(Phase.BUILD)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
