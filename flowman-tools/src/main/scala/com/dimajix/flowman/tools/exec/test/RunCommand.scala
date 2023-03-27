/*
 * Copyright (C) 2018 The Flowman Authors
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
import com.dimajix.common.text.ConsoleColors.red
import com.dimajix.flowman.execution.TestCoordinator


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


    override def execute(session: Session, project: Project, context:Context) : Status = {
        val allTests = if (tests.nonEmpty) {
            tests.flatMap(_.split(",")).toSeq.distinct
        }
        else {
            project.tests.keySet.toSeq
        }

        val coordinator = new TestCoordinator(session, keepGoing, dryRun, parallelism)
        val status = coordinator.execute(project, allTests.map(TestIdentifier.apply))

        if(!status.success) {
            logger.error(red("There have been test failures"))
        }
        status
    }
}
