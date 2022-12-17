/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.testing.LocalSparkSession


class ModuleTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The Module" should "be loadable from a string" in {
        val spec =
            """
              |targets:
              |  blackhole:
              |    kind: blackhole
              |    mapping: input
              |relations:
              |  empty:
              |    kind: empty
              |mappings:
              |  input:
              |    kind: relation
              |    relation: empty
              |environment:
              |  - x=y
              |config:
              |  - spark.lala=lolo
            """.stripMargin
        val module = Module.read.string(spec)
        module.environment should contain("x" -> "y")
        module.config should contain("spark.lala" -> "lolo")
    }

    it should "be executable" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: empty
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: col1
              |          type: string
              |        - name: col2
              |          type: Integer
              |
              |targets:
              |  blackhole:
              |    kind: blackhole
              |    mapping: input
              |
              |mappings:
              |  input:
              |    kind: relation
              |    relation: empty
              |
              |jobs:
              |  default:
              |    targets:
              |     - blackhole
            """.stripMargin
        val project = Module.read.string(spec).toProject("default")
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val runner = session.runner

        val job = context.getJob(JobIdentifier("default"))
        job should not be (null)
        job.name should be ("default")
        job.category should be (Category.JOB)
        job.kind should be ("job")
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)

        session.shutdown()
    }

    it should "set the names of all jobs" in {
        val spec =
            """
              |jobs:
              |  default:
              |    description: "Lala"
            """.stripMargin

        val context = RootContext.builder().build()

        val module = Module.read.string(spec)
        module.jobs.keys should contain ("default")
        val mjob = module.jobs("default").instantiate(context)
        mjob.name should be ("default")

        val project = module.toProject("default")
        project.jobs.keys should contain ("default")
        val pjob = project.jobs("default").instantiate(context)
        pjob.name should be ("default")
    }
}
