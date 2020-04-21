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

package com.dimajix.flowman.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.types.StringType


class SimpleRunnerTest extends FlatSpec with Matchers {
    "The SimpleRunner" should "work" in {
        val session = Session.builder()
            .build()
        val batch = Job.builder(session.context)
            .setName("batch")
            .build()

        val runner = new SimpleRunner
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.SUCCESS)
    }

    it should "catch exceptions" in {
        val session = Session.builder()
            .build()
        val batch = Job.builder(session.context)
            .setName("batch")
            .addParameter("p1", StringType)
            .build()

        val runner = new SimpleRunner
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.FAILED)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.FAILED)
    }
}
