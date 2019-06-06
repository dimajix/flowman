/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.testing.LocalSparkSession


class HiveUnionViewRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "A HiveUnionViewRelation" should "create a view" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
              |    database: default
              |    table: t0
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |        - name: t0_exclusive_col
              |          type: long
              |          
              |  t1:
              |    kind: hiveTable
              |    database: default
              |    table: t1
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: string
              |        - name: additional_col
              |          type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        context.getRelation(RelationIdentifier("t0")).create(executor)
        context.getRelation(RelationIdentifier("t1")).create(executor)

        val schema = context.getRelation(RelationIdentifier("t0")).schema
        val model = HiveUnionViewRelation(
            Relation.Properties(context),
            "default",
            "v0",
            Seq(RelationIdentifier("t0"), RelationIdentifier("t1")),
            schema,
            Seq()
        )

        model.create(executor)
    }
}
