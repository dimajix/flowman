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


class JobTest extends FlatSpec with Matchers {
    "A Job" should "be deseializable from" in {
        val spec =
            """
              |jobs:
              |  job:
              |    tasks:
              |      - type: loop
              |        job: child
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
    }

    it should "support parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        value: v2
              |      - name: p3
              |        type: Integer
              |        value: 7
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
        job.execute(executor, Map("p1" -> "v1")) shouldBe (JobStatus.SUCCESS)
        job.execute(executor, Map("p1" -> "v1", "p2" -> "v2")) shouldBe (JobStatus.SUCCESS)
    }

    it should "fail on undefined parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Map("p2" -> "v1")))
    }

    it should "return correct arguments" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        value: v2
              |      - name: p3
              |        type: Integer
              |        value: 7
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
        job.arguments(Map()) should be (Map("p1" -> null, "p2" -> "v2", "p3" -> "7"))
        job.arguments(Map("p1" -> "lala")) should be (Map("p1" -> "lala", "p2" -> "v2", "p3" -> "7"))
        job.arguments(Map("p2" -> "lala")) should be (Map("p1" -> null, "p2" -> "lala", "p3" -> "7"))
    }
}
