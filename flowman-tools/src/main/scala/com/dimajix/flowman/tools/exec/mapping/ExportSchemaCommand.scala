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

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SchemaWriter


class ExportSchemaCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ExportSchemaCommand])

    @Option(name="-f", aliases=Array("--format"), usage="Specifies the format", metaVar="<format>", required = false)
    var format: String = "spark"
    @Argument(usage = "specifies the mapping to save the schema", metaVar = "<mapping>", required = true)
    var mapping: String = ""
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true)
    var filename: String = ""

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        logger.info(s"Exporting the schema of mapping '$mapping' to '$filename'")

        Try {
            val table = executor.instantiate(MappingIdentifier.parse(mapping))
            val schema = Field.of(table.schema)
            val file = context.fs.local(filename)
            new SchemaWriter(schema).format(format).save(file)
        } match {
            case Success(_) =>
                logger.info("Successfully saved schema")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while save the schema of mapping '$mapping'", e)
                false
        }
    }
}
