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

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class ValidateCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ValidateCommand])

    @Argument(usage = "specifies mappings to validate", metaVar = "<mapping>")
    var mappings: Array[String] = Array()

    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        logger.info("Validating mappings {}", if (mappings != null) mappings.mkString(",") else "all")

        // Then execute output operations
        Try {
            val mappingNames =
                if (mappings.nonEmpty)
                    mappings.toSeq
                else
                    project.mappings.keys.toSeq

            val tables = mappingNames.map(name => context.getMapping(MappingIdentifier(name)))
            tables.forall(table => session.execution.instantiate(table) != null)
        } match {
            case Success(true) =>
                logger.info("Successfully validated mappings")
                true
            case Success(false) =>
                logger.error("Validation of mappings failed")
                false
            case Failure(ex:NoSuchMappingException) =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                false
            case Failure(NonFatal(e)) =>
                logger.error("Caught exception while validating mapping", e)
                false
        }
    }
}
