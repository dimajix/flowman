/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.relation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.ValueConnectionReference
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.VarcharType


class SqlServerRelationTest extends AnyFlatSpec with Matchers {
    "The JdbcTableRelation" should "support embedding the connection" in {
        val spec =
            s"""
               |kind: sqlserver
               |name: some_relation
               |description: "This is a test table"
               |connection:
               |  kind: jdbc
               |  name: some_connection
               |  url: some_url
               |table: lala_001
               |schema:
               |  kind: inline
               |  fields:
               |    - name: str_col
               |      type: string
               |    - name: int_col
               |      type: integer
               |    - name: varchar_col
               |      type: varchar(10)
               |primaryKey:
               |  - str_col
               |""".stripMargin

        val relationSpec = ObjectMapper.parse[RelationSpec](spec).asInstanceOf[SqlServerRelationSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val relation = relationSpec.instantiate(context)
        relation shouldBe a[SqlServerRelation]
        relation.name should be ("some_relation")
        relation.schema should be (Some(InlineSchema(
            Schema.Properties(context, name="embedded", kind="inline"),
            fields = Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType),
                Field("varchar_col", VarcharType(10))
            )
        )))
        relation.fields should be (Seq(
            Field("str_col", StringType, nullable=false),
            Field("int_col", IntegerType),
            Field("varchar_col", VarcharType(10))
        ))
        relation.connection shouldBe a[ValueConnectionReference]
        relation.connection.identifier should be (ConnectionIdentifier("some_connection"))
        relation.connection.name should be ("some_connection")
        relation.primaryKey should be (Seq("str_col"))

        session.shutdown()
    }
}
