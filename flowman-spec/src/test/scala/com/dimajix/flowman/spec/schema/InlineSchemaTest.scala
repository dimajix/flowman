/*
 * Copyright (C) 2018 The Flowman Authors
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


class InlineSchemaTest extends AnyFlatSpec with Matchers {
    "An InlineSchema" should "be parseable with explicit type" in {
        val spec =
            """
              |kind: inline
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: int_col
              |    type: integer
            """.stripMargin

        val session = Session.builder().disableSpark().build()

        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe a[InlineSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe a[InlineSchema]
        result.fields.size should be (2)
        result.fields(0).name should be ("str_col")
        result.fields(1).name should be ("int_col")

        session.shutdown()
    }
}
