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

package com.dimajix.flowman.spec.model

import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.MockedSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class HiveTableRelationTest extends FlatSpec with Matchers with MockedSparkSession  {
    "The HiveTableRelation" should "support create" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col integer
              |)""".stripMargin)
    }

    it should "support single partition columns" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col integer
              |)
              |PARTITIONED BY (spart string)""".stripMargin)
    }

    it should "support multiple partition columns" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |      - name: ip
              |        type: int
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col integer
              |)
              |PARTITIONED BY (spart string, ip integer)""".stripMargin)
    }
}
