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

package com.dimajix.flowman.spec.relation

import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.{types => ftypes}
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
        |    kind: relation
        |    relation: t0
        |""".stripMargin
    val project = Module.read.string(spec).toProject("project")

    val session = Session.builder().withSparkSession(spark).build()
    val execution = session.execution
    val context = session.getContext(project)

    context.getRelation(RelationIdentifier("t0")).create(execution)

    val relation = HiveViewRelation(
      Relation.Properties(context),
      table = TableIdentifier("v0", Some("default")),
      mapping = Some(MappingOutputIdentifier("t0"))
    )

    relation.provides(Operation.CREATE) should be (Set(ResourceIdentifier.ofHiveTable("v0", Some("default"))))
    relation.requires(Operation.CREATE) should be (Set(
        ResourceIdentifier.ofHiveDatabase("default"),
        ResourceIdentifier.ofHiveTable("t0", Some("default"))
    ))
    relation.provides(Operation.WRITE) should be (Set.empty)
    relation.requires(Operation.WRITE) should be (Set.empty)
    relation.provides(Operation.READ) should be (Set(ResourceIdentifier.ofHivePartition("v0", Some("default"), Map())))
    relation.requires(Operation.READ) should be (Set(
        ResourceIdentifier.ofHiveTable("v0", Some("default")),
        ResourceIdentifier.ofHiveTable("t0", Some("default")),
        ResourceIdentifier.ofHivePartition("t0", Some("default"), Map())
    ))
    an[Exception] should be thrownBy(relation.describe(execution))

    // == Create =================================================================================================
    relation.exists(execution) should be (No)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
    relation.loaded(execution, Map()) should be (No)
    relation.create(execution)
    relation.exists(execution) should be (Yes)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
    relation.loaded(execution, Map()) should be (Yes)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (true)

    // == Migrate =================================================================================================
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)
    relation.exists(execution) should be (Yes)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
    relation.migrate(execution, MigrationPolicy.RELAXED)
    relation.exists(execution) should be (Yes)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

    // == Read ===================================================================================================
    relation.describe(execution) should be (ftypes.StructType(Seq(
        Field("str_col", ftypes.StringType),
        Field("int_col", ftypes.IntegerType),
        Field("t0_exclusive_col", ftypes.LongType)
    )))

    // == Destroy ================================================================================================
    relation.destroy(execution)
    relation.exists(execution) should be (No)
    relation.loaded(execution, Map()) should be (No)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (false)

    context.getRelation(RelationIdentifier("t0")).destroy(execution)
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
        |    kind: relation
        |    relation: t0
        | t1:
        |    kind: relation
        |    relation: t1
        | union:
        |    kind: union
        |    inputs:
        |     - t0
        |     - t1
        |""".stripMargin
    val project = Module.read.string(spec).toProject("project")

    val session = Session.builder().withSparkSession(spark).build()
    val execution = session.execution
    val context = session.getContext(project)

    context.getRelation(RelationIdentifier("t0")).create(execution)
    context.getRelation(RelationIdentifier("t1")).create(execution)

    val relation = HiveViewRelation(
      Relation.Properties(context),
      table = TableIdentifier("v0", Some("default")),
      mapping = Some(MappingOutputIdentifier("union"))
    )

    relation.provides(Operation.CREATE) should be (Set(ResourceIdentifier.ofHiveTable("v0", Some("default"))))
    relation.requires(Operation.CREATE) should be (Set(
      ResourceIdentifier.ofHiveDatabase("default"),
      ResourceIdentifier.ofHiveTable("t0", Some("default")),
      ResourceIdentifier.ofHiveTable("t1", Some("default"))
    ))
    relation.provides(Operation.WRITE) should be (Set.empty)
    relation.requires(Operation.WRITE) should be (Set.empty)
    relation.provides(Operation.READ) should be (Set(ResourceIdentifier.ofHivePartition("v0", Some("default"), Map())))
    relation.requires(Operation.READ) should be (Set(
        ResourceIdentifier.ofHiveTable("v0", Some("default")),
        ResourceIdentifier.ofHiveTable("t0", Some("default")),
        ResourceIdentifier.ofHiveTable("t1", Some("default")),
        ResourceIdentifier.ofHivePartition("t0", Some("default"), Map()),
        ResourceIdentifier.ofHivePartition("t1", Some("default"), Map())
    ))

    // == Create =================================================================================================
    relation.exists(execution) should be (No)
    relation.loaded(execution, Map()) should be (No)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
    relation.create(execution)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
    relation.exists(execution) should be (Yes)
    relation.loaded(execution, Map()) should be (Yes)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (true)

    // == Migrate =================================================================================================
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER)
    relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)
    relation.exists(execution) should be (Yes)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
    relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
    relation.migrate(execution, MigrationPolicy.RELAXED)
    relation.exists(execution) should be (Yes)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

    // == Destroy ================================================================================================
    relation.destroy(execution)
    relation.exists(execution) should be (No)
    relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
    relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
    relation.loaded(execution, Map()) should be (No)
    session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (false)

    context.getRelation(RelationIdentifier("t0")).destroy(execution)
    context.getRelation(RelationIdentifier("t1")).destroy(execution)
  }

    it should "work with an external file" in {
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
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val basedir = new Path(Resources.getResource(".").toURI)

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        context.getRelation(RelationIdentifier("t0")).create(execution)

        val relation = HiveViewRelation(
            Relation.Properties(context),
            table = TableIdentifier("v0", Some("default")),
            file = Some(new Path(basedir, "project/relation/some-view.sql"))
        )

        relation.provides(Operation.CREATE) should be (Set(ResourceIdentifier.ofHiveTable("v0", Some("default"))))
        relation.requires(Operation.CREATE) should be (Set(
            ResourceIdentifier.ofHiveDatabase("default"),
            ResourceIdentifier.ofHiveTable("t0", Some("default"))
        ))
        relation.provides(Operation.WRITE) should be (Set.empty)
        relation.requires(Operation.WRITE) should be (Set.empty)
        relation.provides(Operation.READ) should be (Set(ResourceIdentifier.ofHivePartition("v0", Some("default"), Map())))
        relation.requires(Operation.READ) should be (Set(
            ResourceIdentifier.ofHiveTable("v0", Some("default")),
            ResourceIdentifier.ofHiveTable("t0", Some("default")),
            ResourceIdentifier.ofHivePartition("t0", Some("default"), Map())
        ))

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.create(execution)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (true)

        // == Migrate =================================================================================================
        relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
        relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL)
        relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER)
        relation.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL)
        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
        relation.migrate(execution, MigrationPolicy.RELAXED)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        session.catalog.tableExists(TableIdentifier("v0", Some("default"))) should be (false)

        context.getRelation(RelationIdentifier("t0")).destroy(execution)
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
              |    kind: relation
              |    relation: t0
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        context.getRelation(RelationIdentifier("t0")).create(execution)

        val view = HiveViewRelation(
            Relation.Properties(context),
            table = TableIdentifier("table_or_view", Some("default")),
            mapping = Some(MappingOutputIdentifier("t0"))
        )
        val table = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.IntegerType),
                    Field("f3", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table = TableIdentifier("table_or_view", Some("default"))
        )

        // == Create TABLE ============================================================================================
        table.exists(execution) should be (No)
        table.create(execution)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)

        // == Create VIEW ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (view.create(execution, ifNotExists = false))
        table.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        // == Create TABLE ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (table.create(execution, ifNotExists = false))
        table.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        // == Migrate VIEW ==========================================================================================
        view.migrate(execution)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.VIEW)

        // == Destroy VIEW ===========================================================================================
        view.destroy(execution)
        view.exists(execution) should be (No)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table.exists(execution) should be (No)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        session.catalog.tableExists(TableIdentifier("table_or_view", Some("default"))) should be (false)

        // == Destroy VIEW ==========================================================================================
        view.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(view.destroy(execution, ifExists = false))

        // == Destroy TABLE ===========================================================================================
        table.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(table.destroy(execution, ifExists = false))

        context.getRelation(RelationIdentifier("t0")).destroy(execution)
    }

    it should "migrate a view if the schema changes" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val view = HiveViewRelation(
            Relation.Properties(context),
            table = TableIdentifier("view", Some("default")),
            sql = Some("SELECT * FROM table")
        )
        val table = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.IntegerType),
                    Field("f3", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table = TableIdentifier("table", Some("default"))
        )
        val table2 = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f4", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table =TableIdentifier("table", Some("default"))
        )

        // == Create TABLE ============================================================================================
        table.exists(execution) should be (No)
        table.create(execution)
        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).tableType should be (CatalogTableType.MANAGED)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Create VIEW =============================================================================================
        view.exists(execution) should be (No)
        view.create(execution)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Replace TABLE ===========================================================================================
        table.destroy(execution, ifExists = true)
        table2.create(execution)
        table2.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table2.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table2.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).tableType should be (CatalogTableType.MANAGED)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        // == Destroy TABLE ===========================================================================================
        view.destroy(execution, ifExists = true)
        table.destroy(execution, ifExists = true)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
    }

    it should "migrate a view if a column comment changes" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val view = HiveViewRelation(
            Relation.Properties(context),
            table = TableIdentifier("view", Some("default")),
            sql = Some("SELECT * FROM table")
        )
        val table = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.IntegerType, description=Some("This is f3"))
                )
            )),
            table = TableIdentifier("table", Some("default"))
        )
        val table2 = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.IntegerType, description=Some("This is really f3"))
                )
            )),
            table =TableIdentifier("table", Some("default"))
        )
        val table3 = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table =TableIdentifier("table", Some("default"))
        )

        // == Create TABLE ============================================================================================
        table.exists(execution) should be (No)
        table.create(execution)
        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).tableType should be (CatalogTableType.MANAGED)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Create VIEW =============================================================================================
        view.exists(execution) should be (No)
        view.create(execution)
        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Replace TABLE ===========================================================================================
        table.destroy(execution, ifExists = true)
        table2.create(execution)
        table2.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table2.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table2.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).tableType should be (CatalogTableType.MANAGED)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        // == Replace TABLE ===========================================================================================
        table2.destroy(execution, ifExists = true)
        table3.create(execution)
        table3.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table3.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table3.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).tableType should be (CatalogTableType.MANAGED)
        session.catalog.getTable(TableIdentifier("table", Some("default"))).schema should be (table3.schema.get.sparkSchema)

        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table2.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table3.schema.get.sparkSchema)

        // == Migrate VIEW ===========================================================================================
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.migrate(execution)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).tableType should be (CatalogTableType.VIEW)
        session.catalog.getTable(TableIdentifier("view", Some("default"))).schema should be (table3.schema.get.sparkSchema)

        // == Destroy TABLE ===========================================================================================
        view.destroy(execution, ifExists = true)
        table.destroy(execution, ifExists = true)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
    }
}
