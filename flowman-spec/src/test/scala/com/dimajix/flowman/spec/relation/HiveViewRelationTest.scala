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

package com.dimajix.flowman.spec.relation

import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class HiveViewRelationTest extends FlatSpec with Matchers with LocalSparkSession {
  "A HiveViewRelation" should "be creatable from a mapping" in {
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
        |mappings:
        |  t0:
        |    kind: readRelation
        |    relation: t0
        |""".stripMargin
    val project = Module.read.string(spec).toProject("project")

    val session = Session.builder().withSparkSession(spark).build()
    val executor = session.execution
    val context = session.getContext(project)

    context.getRelation(RelationIdentifier("t0")).create(executor)

    val relation = new HiveViewRelation(
      Relation.Properties(context),
      Some("default"),
      "v0",
      Seq(),
      None,
      Some(MappingOutputIdentifier("t0"))
    )

    relation.provides should be (Set(ResourceIdentifier.ofHiveTable("v0", Some("default"))))
    relation.requires should be (Set(
        ResourceIdentifier.ofHiveDatabase("default"),
        ResourceIdentifier.ofHiveTable("t0", Some("default")),
        ResourceIdentifier.ofHivePartition("t0", Some("default"), Map())
    ))
    relation.resources() should be (Set())

    relation.exists(executor) should be (No)
    relation.loaded(executor, Map()) should be (No)
    relation.create(executor)
    relation.exists(executor) should be (Yes)
    relation.loaded(executor, Map()) should be (Yes)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (true)

    relation.destroy(executor)
    relation.exists(executor) should be (No)
    relation.loaded(executor, Map()) should be (No)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (false)

    context.getRelation(RelationIdentifier("t0")).destroy(executor)
  }

  it should "work with non-trivial mappings" in {
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
        |mappings:
        | t0:
        |    kind: readRelation
        |    relation: t0
        | t1:
        |    kind: readRelation
        |    relation: t1
        | union:
        |    kind: union
        |    inputs:
        |     - t0
        |     - t1
        |""".stripMargin
    val project = Module.read.string(spec).toProject("project")

    val session = Session.builder().withSparkSession(spark).build()
    val executor = session.execution
    val context = session.getContext(project)

    context.getRelation(RelationIdentifier("t0")).create(executor)
    context.getRelation(RelationIdentifier("t1")).create(executor)

    val relation = HiveViewRelation(
      Relation.Properties(context),
      Some("default"),
      "v0",
      Seq(),
      None,
      Some(MappingOutputIdentifier("union"))
    )

    relation.provides should be (Set(ResourceIdentifier.ofHiveTable("v0", Some("default"))))
    relation.requires should be (Set(
      ResourceIdentifier.ofHiveDatabase("default"),
      ResourceIdentifier.ofHiveTable("t0", Some("default")),
      ResourceIdentifier.ofHivePartition("t0", Some("default"), Map()),
      ResourceIdentifier.ofHiveTable("t1", Some("default")),
      ResourceIdentifier.ofHivePartition("t1", Some("default"), Map())
    ))
    relation.resources() should be (Set())

    relation.exists(executor) should be (No)
    relation.loaded(executor, Map()) should be (No)
    relation.create(executor)
    relation.exists(executor) should be (Yes)
    relation.loaded(executor, Map()) should be (Yes)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (true)

    //session.catalog.getTable(TableIdentifier("v0", Some("default"))).viewText.foreach(println)

    relation.destroy(executor)
    relation.exists(executor) should be (No)
    relation.loaded(executor, Map()) should be (No)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (false)

    context.getRelation(RelationIdentifier("t0")).destroy(executor)
    context.getRelation(RelationIdentifier("t1")).destroy(executor)
  }
}
