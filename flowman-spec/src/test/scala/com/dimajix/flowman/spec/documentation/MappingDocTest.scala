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
import com.dimajix.flowman.documentation.NotNullColumnCheck
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class MappingDocTest extends AnyFlatSpec with Matchers {
    "A MappingDocSpec" should "be deserializable" in {
        val yaml =
            """
              |description: "This is a mapping"
              |columns:
              |  - name: col_a
              |    description: "This is column a"
              |    checks:
              |      - kind: notNull
              |outputs:
              |  other:
              |     description: "This is an additional output"
              |     columns:
              |       - name: col_x
              |         description: "Column of other output"
              |""".stripMargin

        val spec = ObjectMapper.parse[MappingDocSpec](yaml)

        val context = RootContext.builder().build()
        val mapping = spec.instantiate(context)

        mapping.description should be (Some("This is a mapping"))

        val main = mapping.outputs.find(_.name == "main").get
        main.description should be (None)
        val mainSchema = main.schema.get
        mainSchema.columns.size should be (1)
        mainSchema.columns(0).name should be ("col_a")
        mainSchema.columns(0).description should be (Some("This is column a"))
        mainSchema.columns(0).checks.size should be (1)
        mainSchema.columns(0).checks(0) shouldBe a[NotNullColumnCheck]
        mainSchema.checks.size should be (0)

        val other = mapping.outputs.find(_.name == "other").get
        other.description should be (Some("This is an additional output"))
        val otherSchema = other.schema.get
        otherSchema.columns.size should be (1)
        otherSchema.columns(0).name should be ("col_x")
        otherSchema.columns(0).description should be (Some("Column of other output"))
        otherSchema.columns(0).checks.size should be (0)
        otherSchema.checks.size should be (0)
    }
}
