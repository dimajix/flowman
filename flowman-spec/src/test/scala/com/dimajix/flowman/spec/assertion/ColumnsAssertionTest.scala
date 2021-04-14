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

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class ColumnsAssertionTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ColumnsAssertion" should "be parsable" in {
        val spec =
            """
              |kind: columns
              |mapping: lala
              |expected:
              | - network IS PRESENT
              | - xyz IS ABSENT
              | -   campaign   IS   OF   TYPE   (  int  ,  BIGINT  )
              | - lineitem IS OF TYPE float
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[ColumnsAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[ColumnsAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("lala"))
        assertion.expected should be (Seq(
            ColumnsAssertion.ColumnIsPresent("network"),
            ColumnsAssertion.ColumnIsAbsent("xyz"),
            ColumnsAssertion.ColumnIsOfType("campaign", Seq(IntegerType, LongType)),
            ColumnsAssertion.ColumnIsOfType("lineitem", Seq(FloatType))
        ))
        assertion.inputs should be (Seq(MappingOutputIdentifier("lala")))
        assertion.requires should be (Set())
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = ColumnsAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            Seq(
                ColumnsAssertion.ColumnIsPresent("ID"),
                ColumnsAssertion.ColumnIsAbsent("id"),
                ColumnsAssertion.ColumnIsAbsent("no_such_column"),
                ColumnsAssertion.ColumnIsOfType("id", Seq(BooleanType)),
                ColumnsAssertion.ColumnIsOfType("id", Seq(StringType, LongType))
            )
        )

        val df = execution.spark.range(2).toDF()

        val result = assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df))
        result should be (Seq(
            AssertionResult("ID IS PRESENT", true),
            AssertionResult("id IS ABSENT", false),
            AssertionResult("no_such_column IS ABSENT", true),
            AssertionResult("id IS OF TYPE BOOLEAN", false),
            AssertionResult("id IS OF TYPE (STRING,BIGINT)", true)
        ))
    }
}
