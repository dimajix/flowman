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

import com.google.common.io.Resources
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ResourcesTestSpec extends FlatSpec with Matchers {
    "Projects as resources" should "be testable" in {
        val runner = Runner.builder
            .withProfile("test")
            .withProject(Resources.getResource("flows/project.yml"))
            .build()

        val result = runner.runJob("main")
        result should be(true)
    }
}
