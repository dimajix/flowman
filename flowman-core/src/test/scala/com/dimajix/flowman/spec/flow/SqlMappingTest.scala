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

package com.dimajix.flowman.spec.flow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TableIdentifier


class SqlMappingTest extends FlatSpec with Matchers with LocalSparkSession {

    "An multi line SQL Script" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    type: provided
              |    table: my_table
              |
              |  t1:
              |    type: sql
              |    sql: |
              |      SELECT x,y
              |
              |      FROM t0
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)
    }

    "Another YAML multi line variant" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    type: provided
              |    table: my_table
              |
              |  t1:
              |    type: sql
              |    sql: "
              |      SELECT x,y
              |
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)
    }

    "Dependencies" should "be correct" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    type: sql
              |    sql: "
              |      SELECT x,y
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)

        val session = Session.builder().withSparkSession(spark).build()
        implicit val icontext = session.context
        val mapping = project.mappings("t1")
        mapping.dependencies should be (Array(TableIdentifier("t0")))
    }

    it should "also be correct with subqueries" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    type: sql
              |    sql: "
              |          WITH current AS (
              |            SELECT
              |              0 AS from_archive,
              |              lip.line_item_id as lineitem_id
              |            FROM fe_lineitem li
              |            INNER JOIN fe_lineitem_placement lip ON lip.line_item_id = li.id
              |          ),
              |          archive AS (
              |            SELECT
              |              1 AS from_archive,
              |              lipa.line_item_id as lineitem_id
              |            FROM fe_lineitem li
              |            INNER JOIN fe_lineitem_placement_archive lipa ON lipa.line_item_id = li.id
              |          ),
              |          merged AS (
              |            SELECT
              |              COALESCE(cur.from_archive, ar.from_archive) AS from_archive,
              |              COALESCE(cur.placement_id, ar.placement_id) AS placement_id
              |            FROM current cur
              |            FULL OUTER JOIN archive ar ON cur.lineitem_id = ar.lineitem_id AND cur.placement_id = ar.placement_id
              |          )
              |          SELECT
              |            from_archive,
              |            placement_id
              |          FROM merged lipa
              |     "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)

        val session = Session.builder().withSparkSession(spark).build()
        implicit val icontext = session.context
        val table = project.mappings("t1")
        table.dependencies.map(_.name).sorted should be (Array("fe_lineitem", "fe_lineitem_placement", "fe_lineitem_placement_archive"))
    }
}
