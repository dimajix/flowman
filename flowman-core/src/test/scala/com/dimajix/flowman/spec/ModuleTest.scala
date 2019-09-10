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

package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.spark.testing.LocalSparkSession

class ModuleTest extends FlatSpec with Matchers with LocalSparkSession {
    "The Module" should "be loadable from a string" in {
        val spec =
            """
              |targets:
              |  blackhole:
              |    kind: blackhole
              |    input: input
              |relations:
              |  empty:
              |    kind: null
              |mappings:
              |  input:
              |    kind: read
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
              |    kind: null
              |
              |targets:
              |  blackhole:
              |    kind: blackhole
              |    input: input
              |
              |mappings:
              |  input:
              |    kind: read
              |    relation: empty
              |    columns:
              |      col1: String
              |      col2: Integer
              |
              |batches:
              |  default:
              |    tasks:
              |      - kind: build
              |        targets: blackhole
            """.stripMargin
        val project = Module.read.string(spec).toProject("default")
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.executor
        val runner = executor.runner

        val batch = context.getBatch(BatchIdentifier("default"))
        batch should not be (null)
        batch.name should be ("default")
        batch.category should be ("batch")
        batch.kind should be ("batch")
        runner.executeBatch(executor, batch, Phase.BUILD) should be (Status.SUCCESS)
    }

    it should "set the names of all batches" in {
        val spec =
            """
              |batches:
              |  default:
              |    description: "Lala"
            """.stripMargin

        val context = RootContext.builder().build()

        val module = Module.read.string(spec)
        module.batches.keys should contain ("default")
        val mjob = module.batches("default").instantiate(context)
        mjob.name should be ("default")

        val project = module.toProject("default")
        project.batches.keys should contain ("default")
        val pjob = project.batches("default").instantiate(context)
        pjob.name should be ("default")
    }
}
