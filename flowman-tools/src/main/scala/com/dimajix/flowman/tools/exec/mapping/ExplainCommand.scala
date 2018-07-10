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
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class ExplainCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Option(name="-e", aliases=Array("--extended"), required = false)
    var extended: Boolean = false
    @Argument(usage = "specifies the mapping to explain", metaVar = "<tablename>", required = true)
    var tablename: String = ""


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info(s"Explaining mapping $tablename")

        Try {
            val table = executor.instantiate(MappingIdentifier.parse(tablename))
            table.explain(extended)
        } match {
            case Success(_) =>
                logger.info("Successfully finished explaining mapping")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while explaining mapping '$tablename", e)
                false
        }
    }
}
