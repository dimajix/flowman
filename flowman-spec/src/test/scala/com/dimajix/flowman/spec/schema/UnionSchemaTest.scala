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

package com.dimajix.flowman.spec.schema

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StringType


class UnionSchemaTest extends AnyFlatSpec with Matchers {
    "A UnionSchema" should "work" in {
        val spec =
            """
              |kind: union
              |schemas:
              |  - kind: embedded
              |    fields:
              |      - name: str_col
              |        type: string
              |        nullable: false
              |      - name: int_col
              |        type: integer
              |  - kind: embedded
              |    fields:
              |      - name: int_col
              |        type: boolean
              |      - name: other_col
              |        type: string
              |""".stripMargin

        val session = Session.builder().build()

        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe a[UnionSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe a[UnionSchema]
        result.fields should be (Seq(
            Field("str_col", StringType, nullable = true),
            Field("int_col", StringType, nullable = true),
            Field("other_col", StringType, nullable = true)
        ))
    }
}
