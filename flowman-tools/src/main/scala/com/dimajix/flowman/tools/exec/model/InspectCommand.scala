/*
 * Copyright 2021 Kaya Kupferschmidt
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


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(required = true, usage = "specifies relation to inspect", metaVar = "<relation>")
    var relation: String = ""

    override def execute(session: Session, project: Project, context: Context): Boolean = {
        try {
            val relation = context.getRelation(RelationIdentifier(this.relation))
            println("Relation:")
            println(s"    name: ${relation.name}")
            println(s"    kind: ${relation.kind}")
            println(s"  Requires:")
                relation.requires
                    .map(_.toString)
                    .toSeq.sorted
                    .foreach{ p => println(s"    $p") }
            println(s"  Provides:")
            relation.provides
                .map(_.toString)
                .toSeq.sorted
                .foreach{ p => println(s"    $p") }
            true
        }
        catch {
            case ex:NoSuchRelationException =>
                logger.error(s"Cannot resolve relation '${ex.relation}'")
                false
            case NonFatal(e) =>
                logger.error(s"Error inspecting '$relation': ${reasons(e)}")
                false
        }
    }
}
