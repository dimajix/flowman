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
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SchemaWriter


class ExportSchemaCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ExportSchemaCommand])

    @Option(name="-f", aliases=Array("--format"), usage="Specifies the format", metaVar="<format>", required = false)
    var format: String = "spark"
    @Option(name="-s", aliases=Array("--spark"), usage = "use Spark to derive final schema")
    var useSpark: Boolean = false
    @Argument(usage = "specifies the mapping to save the schema", metaVar = "<mapping>", required = true, index = 0)
    var mapping: String = ""
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true, index = 1)
    var filename: String = ""

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        logger.info(s"Exporting the schema of mapping '$mapping' to '$filename'")

        Try {
            val id = MappingOutputIdentifier(mapping)
            val instance = context.getMapping(id.mapping)
            val executor = session.execution
            val schema =
                if (useSpark) {
                    val table = executor.instantiate(instance, id.output)
                    Field.of(table.schema)
                }
                else {
                    executor.describe(instance, id.output).fields
                }

            val file = context.fs.local(filename)
            new SchemaWriter(schema).format(format).save(file)
        } match {
            case Success(_) =>
                logger.info("Successfully saved schema")
                true
            case Failure(ex:NoSuchMappingException) =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                false
            case Failure(NonFatal(e)) =>
                logger.error(s"Caught exception while save the schema of mapping '$mapping'", e)
                false
        }
    }
}
