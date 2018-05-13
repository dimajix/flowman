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

package com.dimajix.flowman.plugin.example

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.Module


class HelloWorldTaskTest extends FlatSpec with Matchers {
    "A HelloWorldTask" should "be deserializable" in {
        val spec =
            """
              |jobs:
              |  custom:
              |    tasks:
              |      - kind: hello-world
            """.stripMargin
        val module = Module.read.string(spec)
        module.jobs.keys should contain("custom")
        val job = module.jobs("custom")
        job.tasks(0) shouldBe an[HelloWorldTask]
    }
}
