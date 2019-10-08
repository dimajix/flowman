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
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Option(name = "-s", aliases=Array("--spark"), usage = "use Spark to derive final schema")
    var useSpark: Boolean = false
    @Argument(usage = "specifies the mapping to describe", metaVar = "<mapping>", required = true)
    var mapping: String = ""

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        Try {
            val identifier = MappingOutputIdentifier(this.mapping)

            if (useSpark) {
                val mapping = context.getMapping(identifier.mapping)
                val df = executor.instantiate(mapping, identifier.output)
                df.printSchema()
            }
            else {
                val schema = MappingUtils.describe(context, identifier)
                schema match {
                    case Some(s) => s.printTree()
                    case None => logger.error(s"Cannot infer schema of mapping output '${identifier}'")
                }
            }
        } match {
            case Success(_) =>
                logger.info("Successfully finished describing mapping")
                true
            case Failure(ex:NoSuchMappingException) =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                false
            case Failure(e) =>
                logger.error(s"Caught exception while describing mapping '$mapping'", e)
                false
        }
    }
}
