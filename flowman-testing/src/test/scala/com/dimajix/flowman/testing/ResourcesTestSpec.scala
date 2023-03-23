/*
 * Copyright (C) 2019 The Flowman Authors
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

import com.google.common.io.Resources
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase


class ResourcesTestSpec extends AnyFlatSpec with Matchers {
    "Projects as resources" should "be testable" in {
        val runner = Runner.builder
            .withEnvironment("env", "some_value")
            .withProfile("test")
            .withProject(Resources.getResource("flows/project.yml"))
            .build()

        val result = runner.runJob("main", Seq(Phase.BUILD))
        result should be(true)
    }
}
