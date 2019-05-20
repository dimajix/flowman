/*
 * Copyright 2018 Kaya Kupferschmidt
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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType


class MappingSchemaTest extends FlatSpec with Matchers {
    "A MappingSchema" should "resolve the correct schema" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |mappings:
              |  read:
              |    kind: read
              |    relation: empty
              |    partitions:
              |      spart: abc
              |  alias:
              |    kind: alias
              |    input: read
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val schema = MappingSchema(context, "alias")

        schema.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("spart", StringType, false)
        ))
    }

    it should "work as a schema of a relations" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |  sink:
              |    kind: null
              |    schema:
              |      kind: mapping
              |      mapping: alias
              |mappings:
              |  read:
              |    kind: read
              |    relation: empty
              |    partitions:
              |      spart: abc
              |  alias:
              |    kind: alias
              |    input: read
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val sink = context.getRelation(RelationIdentifier("sink"))
        val schema = sink.schema

        schema.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("spart", StringType, false)
        ))
    }
}
