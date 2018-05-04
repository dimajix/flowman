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

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session

class ModuleTest extends FlatSpec with Matchers with LocalSparkSession {
    "The Module" should "be loadable from a string" in {
        val spec =
            """
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
              |outputs:
              |  blackhole:
              |    kind: blackhole
              |    input: input
              |
              |mappings:
              |  input:
              |    kind: read
              |    source: empty
              |    columns:
              |      col1: String
              |      col2: Integer
              |
              |jobs:
              |  default:
              |    tasks:
              |      - kind: output
              |        outputs: blackhole
            """.stripMargin
        val project = Module.read.string(spec).toProject("default")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)
        val context = executor.context
        val runner = context.runner
        runner.execute(executor, project.jobs("default"))
    }

    it should "set the names of all jobs" in {
        val spec =
            """
              |jobs:
              |  default:
              |    description: "Lala"
            """.stripMargin

        val module = Module.read.string(spec)
        module.jobs.keys should contain ("default")
        module.jobs("default").name should be ("default")
        val project = module.toProject("default")
        project.jobs.keys should contain ("default")
        project.jobs("default").name should be ("default")
    }
}
