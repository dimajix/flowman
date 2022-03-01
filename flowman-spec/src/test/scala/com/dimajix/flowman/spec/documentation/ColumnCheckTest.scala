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
import com.dimajix.flowman.documentation.ExpressionColumnCheck
import com.dimajix.flowman.documentation.RangeColumnCheck
import com.dimajix.flowman.documentation.UniqueColumnCheck
import com.dimajix.flowman.documentation.ValuesColumnCheck
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class ColumnCheckTest extends AnyFlatSpec with Matchers {
    "A ColumnCheck" should "be deserializable" in {
        val yaml =
            """
              |kind: unique
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnCheckSpec](yaml)
        spec shouldBe a[UniqueColumnCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (UniqueColumnCheck(
            Some(ColumnReference(None, "col0"))
        ))
    }

    "A RangeColumnCheck" should "be deserializable" in {
        val yaml =
            """
              |kind: range
              |lower: 7
              |upper: 23
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnCheckSpec](yaml)
        spec shouldBe a[RangeColumnCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (RangeColumnCheck(
            Some(ColumnReference(None, "col0")),
            lower="7",
            upper="23"
        ))
    }

    "A ValuesColumnCheck" should "be deserializable" in {
        val yaml =
            """
              |kind: values
              |values: ['a', 12, null]
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnCheckSpec](yaml)
        spec shouldBe a[ValuesColumnCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (ValuesColumnCheck(
            Some(ColumnReference(None, "col0")),
            values = Seq("a", "12", null)
        ))
    }

    "A ExpressionColumnCheck" should "be deserializable" in {
        val yaml =
            """
              |kind: expression
              |expression: "col1 < col2"
            """.stripMargin

        val spec = ObjectMapper.parse[ColumnCheckSpec](yaml)
        spec shouldBe a[ExpressionColumnCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, ColumnReference(None, "col0"))
        test should be (ExpressionColumnCheck(
            Some(ColumnReference(None, "col0")),
            expression = "col1 < col2"
        ))
    }
}
