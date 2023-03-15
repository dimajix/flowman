/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.execution

import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


class TestCoordinator(session:Session, keepGoing:Boolean=false, dryRun:Boolean=false, parallelism:Int= -1) {
    private val logger = session.loggerFactory.getLogger(classOf[TestCoordinator].getName)

    def execute(project: Project, tests: Seq[TestIdentifier]): Status = {
        val context = session.getContext(project)
        if (parallelism > 1)
            executeParallel(context, tests)
        else
            executeLinear(context, tests)
    }

    private def executeLinear(context: Context, tests: Seq[TestIdentifier]): Status = {
        Status.ofAll(tests, keepGoing = keepGoing) { test =>
            val instance = context.getTest(test)
            executeSingle(instance)
        }
    }

    private def executeParallel(context: Context, tests: Seq[TestIdentifier]): Status = {
        Status.parallelOfAll(tests, parallelism, keepGoing = keepGoing, prefix = "TestExecution") { test =>
            val instance = context.getTest(test)
            executeSingle(instance)
        }
    }

    private def executeSingle(test: Test): Status = {
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
