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

package com.dimajix.flowman.tools.exec.target

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchTargetException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.tools.exec.Command


class DependencyTreeCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[DependencyTreeCommand])

    @Argument(required = true, usage = "specifies target to inspect", metaVar = "<target>")
    var target: String = ""

    override def execute(session: Session, project: Project, context: Context): Boolean = {
        try {
            val target = context.getTarget(TargetIdentifier(this.target))
            val graph = new GraphBuilder(context, Phase.BUILD).addTarget(target).build()
            val node = graph.target(target)
            println(node.upstreamDependencyTree)
            true
        }
        catch {
            case ex:NoSuchTargetException =>
                logger.error(s"Cannot resolve target '${ex.target}'")
                false
            case NonFatal(e) =>
                logger.error(s"Error '$target': ${e.getMessage}")
                false
        }

    }
}
