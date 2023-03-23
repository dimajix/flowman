/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.tools.exec.mapping

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class DependencyTreeCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[DependencyTreeCommand])

    @Argument(required = true, usage = "specifies mapping to inspect", metaVar = "<mapping>")
    var mapping: String = ""

    override def execute(session: Session, project: Project, context: Context): Status = {
        try {
            val mapping = context.getMapping(MappingIdentifier(this.mapping))
            val graph = new GraphBuilder(context, Phase.BUILD).addMapping(mapping).build()
            val node = graph.mapping(mapping)
            println(node.upstreamDependencyTree)
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchMappingException =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error in analyzing dependencies of '$mapping':\n  ${reasons(e)}")
                Status.FAILED
        }

    }
}
