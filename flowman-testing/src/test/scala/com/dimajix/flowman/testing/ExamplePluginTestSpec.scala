/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.testing

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase


class ExamplePluginTestSpec extends FlatSpec with Matchers {
    "The example project" should "be testable" in {
        val runner = Runner.builder
            .withProfile("test")
            .withProject(new File("../examples/plugin-example"))
            .build()

        val result = runner.runJob("main", Seq(Phase.BUILD))
        result should be(true)
    }
}
