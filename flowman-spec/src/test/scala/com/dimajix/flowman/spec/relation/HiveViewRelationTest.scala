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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.spark.testing.LocalSparkSession


class HiveViewRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

    val relation = HiveViewRelation(
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

    it should "replace an existing Hive table with a Hive view" in {
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
        val execution = session.execution
        val context = session.getContext(project)

        context.getRelation(RelationIdentifier("t0")).create(execution)

        val view = HiveViewRelation(
            Relation.Properties(context),
            database = Some("default"),
            table = "table_or_view",
            mapping = Some(MappingOutputIdentifier("t0"))
        )
        val table = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.IntegerType),
                    Field("f3", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table = "table_or_view",
            database = Some("default")
        )

        // == Create TABLE ============================================================================================
        table.exists(execution) should be (No)
        table.create(execution)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        table.exists(execution) should be (Yes)
        view.exists(execution) should be (Yes)

        // == Create VIEW ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (view.create(execution, ifNotExists = false))
        table.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        // == Create TABLE ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (table.create(execution, ifNotExists = false))
        table.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        // == Migrate VIEW ==========================================================================================
        view.migrate(execution)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.VIEW)

        // == Destroy VIEW ===========================================================================================
        view.destroy(execution)
        view.exists(execution) should be (No)
        table.exists(execution) should be (No)
        session.catalog.tableExists(TableIdentifier("table_or_view", Some("default"))) should be (false)

        // == Destroy TABLE ===========================================================================================
        table.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(table.destroy(execution, ifExists = false))

        // == Destroy VIEW ==========================================================================================
        view.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(view.destroy(execution, ifExists = false))

        context.getRelation(RelationIdentifier("t0")).destroy(execution)
    }
}
