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

package com.dimajix.flowman.tools.exec.mapping

import scala.util.control.NonFatal

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Status


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(required = true, usage = "specifies mapping to inspect", metaVar = "<mapping>")
    var mapping: String = ""

    override def execute(session: Session, project: Project, context: Context): Status = {
        try {
            val mapping = context.getMapping(MappingIdentifier(this.mapping))
            println("Mapping:")
            println(s"    name: ${mapping.name}")
            println(s"    kind: ${mapping.kind}")
            println(s"    inputs: ${mapping.inputs.map(_.toString).mkString(",")}")
            println(s"    outputs: ${mapping.outputs.mkString(",")}")
            println(s"    cache: ${mapping.cache.toString()}")
            println(s"    broadcast: ${mapping.broadcast}")
            println(s"    checkpoint: ${mapping.checkpoint}")
            println(s"  Requires:")
                mapping.requires
                    .map(_.text)
                    .toSeq.sorted
                    .foreach{ p => println(s"    $p") }
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchMappingException =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error inspecting '$mapping':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}
