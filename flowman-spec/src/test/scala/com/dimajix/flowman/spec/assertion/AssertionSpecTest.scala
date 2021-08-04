/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.assertion

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.spec.annotation.RelationType


@RelationType(kind = "annotatedAssertion")
class AnnotationAssertionSpec extends AssertionSpec {
    override def instantiate(context: Context): Assertion = ???
}

class AssertionSpecTest extends AnyFlatSpec with Matchers {
    "AssertionSpec" should "support custom assertions" in {
        val spec =
            """
              |tests:
              |  main:
              |    assertions:
              |      custom:
              |        kind: annotatedAssertion
              |        description: This is a test
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val test = context.getTest(TestIdentifier("main"))
        test.assertions("custom") shouldBe a[AnnotationAssertionSpec]
    }
}
