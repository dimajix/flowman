/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


class MappingSchemaTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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
              |    kind: relation
              |    relation: empty
              |    partitions:
              |      spart: abc
              |  alias:
              |    kind: alias
              |    input: read
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val schema = MappingSchema(context, "alias")

        schema.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("spart", StringType, false)
        ))
        schema.requires should be (Set())
    }

    it should "work with non-trivial schema inference" in {
        val spec =
            """
              |mappings:
              |  sql:
              |    kind: sql
              |    sql: "
              |     SELECT
              |         'x1' AS char_col,
              |         CAST('x1' AS STRING) AS str_col,
              |         12 AS int_col
              |    "
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)

        val schema = MappingSchema(context, "sql")

        schema.fields should be (Seq(
            Field("char_col", CharType(2), nullable=false),
            Field("str_col", StringType, nullable=false),
            Field("int_col", IntegerType, nullable=false)
        ))
        schema.requires should be (Set())
    }

    it should "work as a schema of a relations" in {
        val spec =
            """
              |relations:
              |  some_hive_table:
              |    kind: hiveTable
              |    table: lala
              |    database: some_db
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
              |    kind: relation
              |    relation: some_hive_table
              |    partitions:
              |      spart: abc
              |  alias:
              |    kind: alias
              |    input: read
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val sink = context.getRelation(RelationIdentifier("sink"))
        val schema = sink.schema.get

        schema.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("spart", StringType, false)
        ))
        schema.requires should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("some_db"))))
    }
}
