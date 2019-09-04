/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class BuildTargetTaskTest extends FlatSpec with Matchers with LocalSparkSession {
    "The BuildTargetTask" should "support configuration via YML" in {
        val spec =
            """
              |jobs:
              |  dump:
              |    description: "Runs all outputs"
              |    tasks:
              |      - kind: build
              |        targets:
              |          - measurements
            """.stripMargin
        val module = Module.read.string(spec)
        val session = Session.builder().build()

        module.jobs.size should be (1)
        val job = module.jobs("dump").instantiate(session.context)
        job.tasks.size should be (1)
        val task = job.tasks(0)
        task shouldBe a[BuildTargetTaskSpec]
    }

    it should "throw exceptions on missing targets" in {
        val session = Session.builder().withSparkSession(spark).build()

        val task = BuildTargetTask(session.context, Seq(TargetIdentifier("no_such_output")), "test")
        an[Exception] shouldBe thrownBy(task.execute(session.executor))
    }

    it should "return false if the target itself has problems" in {
        val spec =
            """
              |targets:
              |  error:
              |    kind: console
              |    input: no_such_mapping
              |jobs:
              |  dump:
              |    description: "Runs all outputs"
              |    tasks:
              |      - kind: build
              |        targets:
              |          - error
            """.stripMargin

        val module = Module.read.string(spec)
        val project = module.toProject("test")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val job = project.jobs("dump").instantiate(session.context)
        job.execute(executor, Map()) should be(Status.FAILED)
    }
}
