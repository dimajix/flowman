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

package com.dimajix.flowman.tools.exec.target

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ValidateCommand])

    @Argument(usage = "specifies target to validate", metaVar = "<output>")
    var outputs: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "validates all targets, even the disabled ones")
    var all: Boolean = false

    def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Validating targets {}", if (outputs != null) outputs.mkString(",") else "all")

        // Then execute output operations
        Try {
            val outputNames =
                if (all)
                    project.targets.keys.toSeq
                else if (outputs.nonEmpty)
                    outputs.toSeq
                else
                    project.targets.filter(_._2.enabled).keys.toSeq

            val tables = outputNames.map(project.targets).flatMap(_.dependencies)
            tables.forall(table => executor.instantiate(table) != null)
        } match {
            case Success(true) =>
                logger.info("Successfully validated targets")
                true
            case Success(false) =>
                logger.error("Validation of targets failed")
                false
            case Failure(e) =>
                logger.error("Caught exception while validating targets", e)
                false
        }
    }
}
