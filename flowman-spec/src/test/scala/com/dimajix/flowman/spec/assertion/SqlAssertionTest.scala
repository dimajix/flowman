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


class SqlAssertionTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The SqlAssertion" should "be parseable" in {
        val spec =
            """
              |kind: sql
              |tests:
              | - query: SELECT * FROM lala
              |   expected: A
              | - query: SELECT * FROM lala
              |   expected: [A]
              | - query: SELECT * FROM lolo
              |   expected: [A,2]
              | - query: SELECT * FROM lolo
              |   expected: [[A,2]]
              | - query: SELECT * FROM lolo
              |   expected:
              |     - A
              |     - B
              | - query: SELECT * FROM lolo
              |   expected:
              |     - [A]
              |     - [B]
              | - query: SELECT * FROM lolo
              |   expected:
              |     - [A,1]
              |     - [B,2]
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[SqlAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[SqlAssertion]
        assertion.name should be ("")
        assertion.tests should be (Seq(
            SqlAssertion.Case(
                query = "SELECT * FROM lala",
                expected = Seq(Array("A"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lala",
                expected = Seq(Array("A"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A"), Array("2"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A", "2"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A"), Array("B"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A"), Array("B"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A", "1"), Array("B", "2"))
            )
        ))
        assertion.inputs should be (Set(MappingOutputIdentifier("lala"), MappingOutputIdentifier("lolo")))
        assertion.requires should be (Set.empty)
    }

    it should "support a single top level test" in {
        val spec =
            """
              |kind: sql
              |query: SELECT * FROM lala
              |expected: A
              |tests:
              | - query: SELECT * FROM lolo
              |   expected: [A]
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[SqlAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[SqlAssertion]
        assertion.name should be ("")
        assertion.tests should be (Seq(
            SqlAssertion.Case(
                query = "SELECT * FROM lala",
                expected = Seq(Array("A"))
            ),
            SqlAssertion.Case(
                query = "SELECT * FROM lolo",
                expected = Seq(Array("A"))
            )
        ))
        assertion.inputs should be (Set(MappingOutputIdentifier("lala"), MappingOutputIdentifier("lolo")))
        assertion.requires should be (Set.empty)

    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SqlAssertion(
            Assertion.Properties(context),
            Seq(
                SqlAssertion.Case(
                    query = "SELECT COUNT(*), SUM(id) FROM mx",
                    expected = Seq(Array("2", "1"))
                ),
                SqlAssertion.Case(
                    query = "SELECT COUNT(*) FROM my",
                    expected = Seq(Array("3"))
                )
            )
        )

        assertion.inputs should be (Set(MappingOutputIdentifier("mx"), MappingOutputIdentifier("my")))
        assertion.requires should be (Set.empty)

        val mx = execution.spark.range(2).toDF()
        val my = execution.spark.range(3).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("mx") -> mx, MappingOutputIdentifier("my") -> my))
        result.withoutTime should be (
            AssertionResult(
                assertion,
                Seq(
                    AssertionTestResult("SELECT COUNT(*), SUM(id) FROM mx", None, true),
                    AssertionTestResult("SELECT COUNT(*) FROM my", None, true)
                )
            ).withoutTime
        )

        session.shutdown()
    }

    it should "fail on too many columns" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SqlAssertion(
            Assertion.Properties(context),
            Seq(
                SqlAssertion.Case(
                    query = "SELECT COUNT(*),SUM(id) FROM mx",
                    expected = Seq(Array("2", "1", "3"))
                )
            )
        )

        assertion.inputs should be (Set(MappingOutputIdentifier("mx")))
        assertion.requires should be (Set.empty)

        val mx = execution.spark.range(2).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("mx") -> mx))
        result.withoutTime should be (
            AssertionResult(
                assertion,
                Seq(
                    AssertionTestResult("SELECT COUNT(*),SUM(id) FROM mx", None, false)
                )
            ).withoutTime
        )

        session.shutdown()
    }

    it should "fail on too few columns" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SqlAssertion(
            Assertion.Properties(context),
            Seq(
                SqlAssertion.Case(
                    query = "SELECT COUNT(*),SUM(id) FROM mx",
                    expected = Seq(Array("2"))
                )
            )
        )

        assertion.inputs should be (Set(MappingOutputIdentifier("mx")))
        assertion.requires should be (Set.empty)

        val mx = execution.spark.range(2).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("mx") -> mx))
        result.withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("SELECT COUNT(*),SUM(id) FROM mx", None, false))
            ).withoutTime
        )

        session.shutdown()
    }

    it should "fail on wrong column types" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SqlAssertion(
            Assertion.Properties(context),
            Seq(
                SqlAssertion.Case(
                    query = "SELECT COUNT(*),SUM(id) FROM mx",
                    expected = Seq(Array("2.0"))
                )
            )
        )

        assertion.inputs should be (Set(MappingOutputIdentifier("mx")))
        assertion.requires should be (Set.empty)

        val mx = execution.spark.range(2).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("mx") -> mx))
        result.withoutTime should be (
            AssertionResult(
                assertion,
                Seq(
                    AssertionTestResult("SELECT COUNT(*),SUM(id) FROM mx", None, false)
                )
            ).withoutTime
        )

        session.shutdown()
    }
}
