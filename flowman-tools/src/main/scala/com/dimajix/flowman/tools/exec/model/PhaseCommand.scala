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
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.target.RelationTargetSpec
import com.dimajix.flowman.tools.exec.ActionCommand


class PhaseCommand(phase:Phase) extends ActionCommand {
    private val logger = LoggerFactory.getLogger(this.getClass)

    @Argument(usage = "specifies relations to create", metaVar = "<relation>")
    var relations: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        logger.info(s"Executing phase '$phase' for relations {}", if (relations != null) relations.mkString(",") else "all")

        val toRun =
            if (relations.nonEmpty)
                relations.toSeq
            else
                project.relations.keys.toSeq

        val jobContext = ScopeContext.builder(context)
            .withTargets(toRun.map(rel => (rel,  RelationTargetSpec(rel, rel))).toMap)
            .build()
        val job = Job.builder(jobContext)
            .setName("cli-create-relations")
            .setDescription("Create relations via CLI")
            .setTargets(toRun.map(t => TargetIdentifier(t)))
            .build()

        val runner = session.runner
        val executor = session.executor
        val result = runner.executeJob(executor, job, Seq(phase))

        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}

class CreateCommand extends PhaseCommand(Phase.CREATE)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
