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

package com.dimajix.flowman.spec.runner

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.StringType
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.JobStatus


class SimpleRunnerTest extends FlatSpec with Matchers {
    "The SimpleRunner" should "work" in {
        val runner = new SimpleRunner
        val job = Job.builder()
            .setName("job")
            .build()
        val session = Session.builder()
            .build()
        runner.execute(session.executor, job) should be (JobStatus.SUCCESS)
        runner.execute(session.executor, job) should be (JobStatus.SUCCESS)
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: simple
            """.stripMargin
        val runner = ObjectMapper.parse[Runner](spec)
        runner shouldBe a[SimpleRunner]
    }

    it should "catch exceptions" in {
        val runner = new SimpleRunner
        val job = Job.builder()
            .setName("job")
            .addParameter("p1", StringType)
            .build()
        val session = Session.builder()
            .build()
        runner.execute(session.executor, job) should be (JobStatus.FAILURE)
        runner.execute(session.executor, job) should be (JobStatus.FAILURE)
    }
}
