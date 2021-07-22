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

package com.dimajix.flowman.tools.exec.test

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.util.ConsoleColors.red


class RunCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(required = false, usage = "specifies tests(s) to execute", metaVar = "<tests>")
    var tests: Array[String] = Array()
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of all targets in case of errors")
    var keepGoing: Boolean = false


    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        Try {
            val allTests = if (tests.nonEmpty) {
                tests.flatMap(_.split(",")).toSeq.distinct
            }
            else {
                project.tests.keySet.toSeq
            }

            Status.ofAll(allTests, true) { test =>
                val runner = session.runner
                val instance = context.getTest(TestIdentifier(test))
                if (instance.assertions.nonEmpty) {
                    runner.executeTest(instance, keepGoing = keepGoing)
                }
                else {
                    logger.info(s"Skipping test '$test' which does not provide any assertions")
                    Status.SUCCESS
                }
            }
        }
        match {
            case Success(Status.SUCCESS) => true
            case Success(Status.SKIPPED) => true
            case Success(_) =>
                logger.error(red("There have been test failures"))
                false
            case Failure(e) =>
                logger.error(e.getMessage)
                false
        }
    }
}
