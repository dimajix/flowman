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

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.assertion.UniqueKeyAssertionTest.Record
import com.dimajix.spark.testing.LocalSparkSession


object UniqueKeyAssertionTest {
    case class Record(id_1: String, id_2: String)
}

class UniqueKeyAssertionTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The UniqueKeyAssertion" should "be parseable" in {
        val spec =
            """
              |kind: uniqueKey
              |mapping: some_mapping
              |key: id
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[UniqueKeyAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[UniqueKeyAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("some_mapping"))
        assertion.key should be (Seq("id"))
    }

    it should "be parseable with a compound key" in {
        val spec =
            """
              |kind: uniqueKey
              |mapping: some_mapping
              |key:
              | - date
              | - from_currency
              | - to_currency
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[UniqueKeyAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[UniqueKeyAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("some_mapping"))
        assertion.key should be (Seq("date", "from_currency", "to_currency"))
    }

    it should "return no error" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = UniqueKeyAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            Seq("id_1", "id_2")
        )

        val df = execution.spark.createDataFrame(Seq(
            Record("1", "1"),
            Record("1", "2"),
            Record("2", "2")
        ))

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df))
        result should be (Seq(AssertionResult("unique_key for 'df:main' with keys 'id_1,id_2'", true)))
    }

    it should "return an error" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = UniqueKeyAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            Seq("id_1", "id_2")
        )

        val df = execution.spark.createDataFrame(Seq(
            Record("1", "1"),
            Record("1", "2"),
            Record("1", "2")
        ))

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df))
        result should be (Seq(AssertionResult("unique_key for 'df:main' with keys 'id_1,id_2'", false)))
    }
}
