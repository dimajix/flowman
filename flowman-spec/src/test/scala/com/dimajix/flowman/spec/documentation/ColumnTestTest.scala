/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.documentation.ColumnReference
import com.dimajix.flowman.documentation.ExpressionColumnTest
import com.dimajix.flowman.documentation.RangeColumnTest
import com.dimajix.flowman.documentation.UniqueColumnTest
import com.dimajix.flowman.documentation.ValuesColumnTest
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class ColumnTestTest extends AnyFlatSpec with Matchers {
    "A ColumnTest" should "be deserializable" in {
        val yaml =
            """
              |kind: unique
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnTestSpec](yaml)
        spec shouldBe a[UniqueColumnTestSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (UniqueColumnTest(
            Some(ColumnReference(None, "col0"))
        ))
    }

    "A RangeColumnTest" should "be deserializable" in {
        val yaml =
            """
              |kind: range
              |lower: 7
              |upper: 23
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnTestSpec](yaml)
        spec shouldBe a[RangeColumnTestSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (RangeColumnTest(
            Some(ColumnReference(None, "col0")),
            lower="7",
            upper="23"
        ))
    }

    "A ValuesColumnTest" should "be deserializable" in {
        val yaml =
            """
              |kind: values
              |values: ['a', 12, null]
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnTestSpec](yaml)
        spec shouldBe a[ValuesColumnTestSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (ValuesColumnTest(
            Some(ColumnReference(None, "col0")),
            values = Seq("a", "12", null)
        ))
    }

    "A ExpressionColumnTest" should "be deserializable" in {
        val yaml =
            """
              |kind: expression
              |expression: "col1 < col2"
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnTestSpec](yaml)
        spec shouldBe a[ExpressionColumnTestSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (ExpressionColumnTest(
            Some(ColumnReference(None, "col0")),
            expression = "col1 < col2"
        ))
    }
}
