/*
 * Copyright (C) 2021 The Flowman Authors
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

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class ExpressionAssertionTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ExpressionAssertion" should "be parsable" in {
        val spec =
            """
              |kind: expression
              |mapping: lala
              |expected:
              | - network IS NOT NULL
              | - campaign IN (1,2)
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[ExpressionAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[ExpressionAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("lala"))
        assertion.expected should be (Seq(
            "network IS NOT NULL",
            "campaign IN (1,2)"
        ))
        assertion.inputs should be (Set(MappingOutputIdentifier("lala")))
        assertion.requires should be (Set.empty)
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = ExpressionAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            Seq(
                "id IS NOT NULL",
                "id IN (4,5)"
            )
        )

        val df = execution.spark.range(2).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df)).withoutTime
        result should be (
            AssertionResult(
                assertion,
                Seq(
                    AssertionTestResult("id IS NOT NULL", None, true),
                    AssertionTestResult("id IN (4,5)", None, false)
                )
            ).withoutTime
        )

        session.shutdown()
    }
}
