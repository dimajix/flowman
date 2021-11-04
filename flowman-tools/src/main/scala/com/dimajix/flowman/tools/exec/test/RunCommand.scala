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
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.util.ConsoleColors.red


class RunCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(required = false, usage = "specifies tests(s) to execute", metaVar = "<tests>")
    var tests: Array[String] = Array()
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of all tests in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    var dryRun: Boolean = false
    @Option(name = "-j", aliases=Array("--jobs"), usage = "number of tests to run in parallel")
    var parallelism: Int = 1


    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        val allTests = if (tests.nonEmpty) {
            tests.flatMap(_.split(",")).toSeq.distinct
        }
        else {
            project.tests.keySet.toSeq
        }

        val status = if (parallelism > 1)
            executeParallel(session, context, allTests)
        else
            executeLinear(session, context, allTests)

        if(!status.success) {
            logger.error(red("There have been test failures"))
        }
        status.success
    }

    private def executeLinear(session: Session, context:Context, tests:Seq[String]) : Status = {
        Status.ofAll(tests, keepGoing=keepGoing) { test =>
            val instance = context.getTest(TestIdentifier(test))
            executeSingle(session, instance)
        }
    }

    private def executeParallel(session: Session, context:Context, tests:Seq[String]) : Status = {
        Status.parallelOfAll(tests, parallelism, keepGoing=keepGoing, prefix="TestExecution") { test =>
            val instance = context.getTest(TestIdentifier(test))
            executeSingle(session, instance)
        }
    }

    private def executeSingle(session: Session, test:Test) : Status = {
        val runner = session.runner
        if (test.assertions.nonEmpty) {
            runner.executeTest(test, keepGoing = keepGoing, dryRun = dryRun)
        }
        else {
            logger.info(s"Skipping test ${test.identifier} which does not provide any assertions")
            Status.SUCCESS
        }
    }
}
