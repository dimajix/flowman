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

package com.dimajix.flowman.spec.test

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TestIdentifier


class TestSpecTest extends AnyFlatSpec with Matchers {
    "A Test" should "be deseializable from" in {
        val spec =
            """
              |tests:
              |  base_test:
              |    environment:
              |      - x=y
              |
              |  test:
              |    description: Some Test
              |    extends:
              |      - base_test
              |
              |    fixtures:
              |      dummy_fixture:
              |        kind: null
              |
              |    overrideMappings:
              |      some_mapping:
              |        kind: mock
              |
              |    overrideRelations:
              |      some_relation:
              |        kind: mock
              |
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val test = context.getTest(TestIdentifier("test"))
        test.name should be ("test")
        test.identifier should be (TestIdentifier("project/test"))
        test.description should be (Some("Some Test"))
        test.targets should be (Seq(TargetIdentifier("grabenv")))
        test.environment should be (Map("x" -> "y"))
        test.fixtures.keySet should be (Set("dummy_fixture"))
        test.overrideMappings.keySet should be (Set("some_mapping"))
        test.overrideRelations.keySet should be (Set("some_relation"))
    }
}
