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

package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TargetIdentifier


class BuildTargetTaskTest extends FlatSpec with Matchers {
    "The BuildTargetTask" should "support string assignments from code" in {
        val session = Session.builder().build()
        implicit val context = session.context
        val task = BuildTargetTask(Seq("lala"), "test")
        task.targets should equal(Seq(TargetIdentifier("lala",None)))
    }
    it should "support configuration via YML" in {
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
        implicit val context = session.context
        module.jobs.size should be (1)
        val job = module.jobs("dump")
        job.tasks.size should be (1)
        val task = job.tasks(0).asInstanceOf[BuildTargetTask]
        task.targets.size should be (1)
    }
}
