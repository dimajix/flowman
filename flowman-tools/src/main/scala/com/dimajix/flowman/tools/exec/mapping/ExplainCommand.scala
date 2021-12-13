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

package com.dimajix.flowman.tools.exec.mapping

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class ExplainCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ExplainCommand])

    @Option(name="-e", aliases=Array("--extended"), required = false)
    var extended: Boolean = false
    @Argument(usage = "specifies the mapping to explain", metaVar = "<mapping>", required = true)
    var mapping: String = ""


    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        logger.info(s"Explaining mapping '$mapping'")

        try {
            val id = MappingOutputIdentifier(mapping)
            val instance = context.getMapping(id.mapping)
            val executor = session.execution
            val table = executor.instantiate(instance, id.output)
            table.explain(extended)
            true
        }
        catch {
            case ex:NoSuchMappingException =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                false
            case NonFatal(e) =>
                logger.error(s"Caught exception while explaining mapping '$mapping", e)
                false
        }
    }
}
