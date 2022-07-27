/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import scala.util.control.NonFatal

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchRelationException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.tools.exec.Command
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Relation


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(required = true, usage = "specifies relation to inspect", metaVar = "<relation>")
    var relation: String = ""

    override def execute(session: Session, project: Project, context: Context): Status = {
        try {
            val relation = context.getRelation(RelationIdentifier(this.relation))
            println("Relation:")
            println(s"    name: ${relation.name}")
            println(s"    kind: ${relation.kind}")
            printDependencies(relation, Operation.CREATE)
            printDependencies(relation, Operation.READ)
            printDependencies(relation, Operation.WRITE)
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchRelationException =>
                logger.error(s"Cannot resolve relation '${ex.relation}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error inspecting '$relation': ${reasons(e)}")
                Status.FAILED
        }
    }
    private def printDependencies(relation:Relation, op:Operation) : Unit = {
        println(s"  Requires - $op:")
        relation.requires(op)
            .map(_.toString)
            .toSeq.sorted
            .foreach{ p => println(s"    $p") }
        println(s"  Provides - $op:")
        relation.provides(op)
            .map(_.toString)
            .toSeq.sorted
            .foreach{ p => println(s"    $p") }

    }
}
