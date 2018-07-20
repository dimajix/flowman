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

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.SparkSchemaUtils


class ExportSchemaCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ExportSchemaCommand])

    @Option(name="-f", aliases=Array("--format"), usage="Specifies the format", metaVar="<format>", required = false)
    var format: String = "spark"
    @Argument(usage = "specifies the mapping to save the schema", metaVar = "<mapping>", required = true)
    var tablename: String = ""
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true)
    var filename: String = ""

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info(s"Exporting the schema of mapping '$tablename' to '$filename'")

        Try {
            val table = executor.instantiate(MappingIdentifier.parse(tablename))
            val schema = SparkSchemaUtils.fromSpark(table.schema)
            val file = executor.context.fs.local(filename)
            new SchemaWriter(schema).format(format).save(file)
        } match {
            case Success(_) =>
                logger.info("Successfully saved schema")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while save the schema of mapping '$tablename'", e)
                false
        }
    }
}
