/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.tools.exec.relation

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.common.ParserUtils
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.target.RelationTarget
import com.dimajix.flowman.tools.exec.Command


class PhaseCommand(phase:Phase) extends Command {
    private val logger = LoggerFactory.getLogger(this.getClass)

    @Argument(required=true, usage = "specifies relations to create", metaVar = "<relation>")
    var relations: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of job with next target in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    var dryRun: Boolean = false
    @Option(name = "-p", aliases=Array("--partition"), usage = "specify partition to work on, as partition1=value1,partition2=value2")
    var partition: String = ""

    override def execute(session: Session, project: Project, context:Context) : Status = {
        logger.info(s"Executing phase '$phase' for relations ${relations.mkString(",")}")

        val toRun = relations.flatMap(_.split(",")).toSeq
        val partition = ParserUtils.parseDelimitedKeyValues(this.partition)

        // Create appropriate targets for all relations
        val targets = toRun.map { rel => {
            val props = Target.Properties(context, rel, "relation")
            RelationTarget(props, RelationIdentifier(rel, project.name), MappingOutputIdentifier.empty, partition).asInstanceOf[Target]
        }}

        val runner = session.runner
        runner.executeTargets(targets, Seq(phase), jobName="cli-tools", force=force, keepGoing=keepGoing, dryRun=dryRun, isolated=false)
    }
}

class CreateCommand extends PhaseCommand(Phase.CREATE)
class VerifyCommand extends PhaseCommand(Phase.VERIFY)
class TruncateCommand extends PhaseCommand(Phase.TRUNCATE)
class DestroyCommand extends PhaseCommand(Phase.DESTROY)
