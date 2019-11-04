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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class HiveUnionTableRelationTest extends FlatSpec with Matchers with LocalSparkSession with QueryTest {
    "The HiveUnionTableRelation" should "support the full lifecycle" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveUnionTable
              |    description: "This is a test table"
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |        - name: char_col
              |          type: char(10)
              |        - name: varchar_col
              |          type: varchar(10)
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("default")), ResourceIdentifier.ofHiveTable("lala_[0-9]+", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_[0-9]+", Some("default"), Map())))
        relation.fields should be(
            Field("str_col", ftypes.StringType) ::
                Field("int_col", ftypes.IntegerType) ::
                Field("char_col", ftypes.CharType(10)) ::
                Field("varchar_col", ftypes.VarcharType(10)) ::
                Nil)

        // == Create ===================================================================
        relation.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)

        // Inspect Hive view
        val view = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view.provider should be (None)
        view.comment should be (None)
        view.identifier should be (TableIdentifier("lala", Some("default")))
        view.tableType should be (CatalogTableType.VIEW)
        view.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                StructField("varchar_col", StringType) ::
                Nil
        ))
        view.partitionColumnNames should be (Seq())
        view.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table = session.catalog.getTable(TableIdentifier("lala_1", Some("default")))
        table.provider should be (Some("hive"))
        table.comment should be(Some("This is a test table"))
        table.identifier should be (TableIdentifier("lala_1", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                StructField("varchar_col", StringType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location.toString should not be ("")

        // Try to create relation, although it already exists
        a[TableAlreadyExistsException] shouldBe thrownBy(relation.create(executor))
        relation.create(executor, true)

        // == Migrate ===================================================================
        relation.migrate(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", 21, "v3", "v4")
        ))
        val df = spark.createDataFrame(rdd, table.schema)
        relation.write(executor, df, Map())

        // == Read ===================================================================
        checkAnswer(relation.read(executor, None), df.collect())

        // == Destroy ===================================================================
        relation.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)

        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
    }

    it should "cast compatible types" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveUnionTable
              |    description: "This is a test table"
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |        - name: char_col
              |          type: char(10)
              |        - name: varchar_col
              |          type: varchar(10)
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        // == Create ===================================================================
        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.create(executor)

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", 21.toShort, true, "v4")
        ))
        val schema = StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", ShortType) ::
                StructField("char_col", BooleanType) ::
                StructField("varchar_col", StringType) ::
                Nil
        )
        val df = spark.createDataFrame(rdd, schema)
        relation.write(executor, df, Map())

        // == Read ===================================================================
        val expected1 = Seq(
            Row("v1", 21, "true", "v4")
        )
        checkAnswer(relation.read(executor, None), expected1)

        // == Write ===================================================================
        val rdd2 = spark.sparkContext.parallelize(Seq(
            Row("v2", 21.toShort, "v4")
        ))
        val schema2 = StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", ShortType) ::
                StructField("varchar_col", StringType) ::
                Nil
        )
        val df2 = spark.createDataFrame(rdd2, schema2)
        relation.write(executor, df2, Map())

        // == Write ===================================================================
        val incompatibleSchema = StructType(
            StructField("int_col", StringType) ::
            Nil
        )
        val incompatibleDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], incompatibleSchema)
        a[Exception] shouldBe thrownBy(relation.write(executor, incompatibleDf, Map()))

        // == Read ===================================================================
        val expected2 = Seq(
            Row("v2", 21, null, "v4")
        )
        checkAnswer(relation.read(executor, None), expected2)

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support partitions" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveUnionTable
              |    description: "This is a test table"
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: partition_col
              |        type: string
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("default")), ResourceIdentifier.ofHiveTable("lala_[0-9]+", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_[0-9]+", Some("default"), Map())))
        relation.fields should be(
            Field("str_col", ftypes.StringType) ::
                Field("int_col", ftypes.IntegerType) ::
                Nil)

        // == Create ===================================================================
        relation.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // Inspect Hive view
        val view = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view.provider should be (None)
        view.comment should be (None)
        view.identifier should be (TableIdentifier("lala", Some("default")))
        view.tableType should be (CatalogTableType.VIEW)
        view.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType) ::
                Nil
        ))
        view.partitionColumnNames should be (Seq())
        view.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table = session.catalog.getTable(TableIdentifier("lala_1", Some("default")))
        table.provider should be (Some("hive"))
        table.comment should be(Some("This is a test table"))
        table.identifier should be (TableIdentifier("lala_1", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq("partition_col"))
        table.partitionSchema should be (StructType(
            StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table.location.toString should not be ("")

        // == Migrate ===================================================================
        relation.migrate(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", 21)
        ))
        val df = spark.createDataFrame(rdd, table.dataSchema)
        relation.write(executor, df, Map("partition_col" -> SingleValue("part_1")))

        // == Read ===================================================================
        val rows = Seq(
            Row("v1", 21, "part_1")
        )
        checkAnswer(relation.read(executor, None, Map()), rows)
        checkAnswer(relation.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows)
        checkAnswer(relation.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), Seq())

        // == Destroy ===================================================================
        relation.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)
    }

    it should "support migration by adding new columns" in {
        val spec =
            """
              |relations:
              |  t1:
              |    kind: hiveUnionTable
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: partition_col
              |        type: string
              |
              |  t2:
              |    kind: hiveUnionTable
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: char_col
              |          type: char(10)
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: partition_col
              |        type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation_1 = context.getRelation(RelationIdentifier("t1"))
        relation_1.provides should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("default")), ResourceIdentifier.ofHiveTable("lala_[0-9]+", Some("default"))))
        relation_1.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation_1.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_[0-9]+", Some("default"), Map())))
        relation_1.fields should be(
            Field("str_col", ftypes.StringType) ::
                Field("int_col", ftypes.IntegerType) ::
                Nil)

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)
        relation_1.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // Inspect Hive view
        val view_1 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view_1.provider should be (None)
        view_1.comment should be (None)
        view_1.identifier should be (TableIdentifier("lala", Some("default")))
        view_1.tableType should be (CatalogTableType.VIEW)
        view_1.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType) ::
                Nil
        ))
        view_1.partitionColumnNames should be (Seq())
        view_1.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table_1 = session.catalog.getTable(TableIdentifier("lala_1", Some("default")))
        table_1.identifier should be (TableIdentifier("lala_1", Some("default")))
        table_1.tableType should be (CatalogTableType.MANAGED)
        table_1.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table_1.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table_1.partitionColumnNames should be (Seq("partition_col"))
        table_1.partitionSchema should be (StructType(
            StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", 21)
        ))
        val df = spark.createDataFrame(rdd, table_1.dataSchema)
        relation_1.write(executor, df, Map("partition_col" -> SingleValue("part_1")))

        // == Migrate ===================================================================
        val relation_2 = context.getRelation(RelationIdentifier("t2"))
        relation_2.migrate(executor)

        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // Inspect Hive view
        val view_2 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view_2.provider should be (None)
        view_2.comment should be (None)
        view_2.identifier should be (TableIdentifier("lala", Some("default")))
        view_2.tableType should be (CatalogTableType.VIEW)
        view_2.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("char_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType) ::
                Nil
        ))
        view_2.partitionColumnNames should be (Seq())
        view_2.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table_2 = session.catalog.getTable(TableIdentifier("lala_1", Some("default")))
        table_2.identifier should be (TableIdentifier("lala_1", Some("default")))
        table_2.tableType should be (CatalogTableType.MANAGED)
        table_2.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table_2.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                Nil
        ))
        table_2.partitionColumnNames should be (Seq("partition_col"))
        table_2.partitionSchema should be (StructType(
            StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))

        // == Write ===================================================================
        val rdd_2 = spark.sparkContext.parallelize(Seq(
            Row("v2", 22, "lala")
        ))
        val df2 = spark.createDataFrame(rdd_2, table_2.dataSchema)
        relation_2.write(executor, df2, Map("partition_col" -> SingleValue("part_2")))

        // == Read ===================================================================
        val rows_1 = Seq(
            Row("v1", null, 21, "part_1")
        )
        val rows_2 = Seq(
            Row("v2", "lala", 22, "part_2")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1 ++ rows_2)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), rows_2)

        // == Overwrite ===================================================================
        val rdd_2a = spark.sparkContext.parallelize(Seq(
            Row("v3", 23, "lala")
        ))
        val df2a = spark.createDataFrame(rdd_2a, table_2.dataSchema)
        relation_2.write(executor, df2a, Map("partition_col" -> SingleValue("part_2")))

        // == Read ===================================================================
        val rows_2a = Seq(
            Row("v3", "lala", 23, "part_2")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1 ++ rows_2a)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), rows_2a)

        // == Destroy ===================================================================
        relation_2.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)
    }

    it should "support migration by creating new tables" in {
        val spec =
            """
              |relations:
              |  t1:
              |    kind: hiveUnionTable
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: partition_col
              |        type: string
              |
              |  t2:
              |    kind: hiveUnionTable
              |    tableDatabase: default
              |    tablePrefix: lala
              |    view: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: boolean
              |    partitions:
              |      - name: partition_col
              |        type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation_1 = context.getRelation(RelationIdentifier("t1"))
        relation_1.provides should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("default")), ResourceIdentifier.ofHiveTable("lala_[0-9]+", Some("default"))))
        relation_1.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation_1.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_[0-9]+", Some("default"), Map())))
        relation_1.fields should be(
            Field("str_col", ftypes.StringType) ::
                Field("int_col", ftypes.IntegerType) ::
                Nil)

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)
        relation_1.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)

        // Inspect Hive view
        val view_1 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view_1.provider should be (None)
        view_1.comment should be (None)
        view_1.identifier should be (TableIdentifier("lala", Some("default")))
        view_1.tableType should be (CatalogTableType.VIEW)
        view_1.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType) ::
                Nil
        ))
        view_1.partitionColumnNames should be (Seq())
        view_1.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table_1 = session.catalog.getTable(TableIdentifier("lala_1", Some("default")))
        table_1.identifier should be (TableIdentifier("lala_1", Some("default")))
        table_1.tableType should be (CatalogTableType.MANAGED)
        table_1.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table_1.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table_1.partitionColumnNames should be (Seq("partition_col"))
        table_1.partitionSchema should be (StructType(
            StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))

        // == Write ===================================================================
        val rdd_1 = spark.sparkContext.parallelize(Seq(
            Row("v1", 21)
        ))
        val df_1 = spark.createDataFrame(rdd_1, table_1.dataSchema)
        relation_1.write(executor, df_1, Map("partition_col" -> SingleValue("part_1")))

        // == Migrate ===================================================================
        val relation_2 = context.getRelation(RelationIdentifier("t2"))
        relation_2.migrate(executor)

        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (true)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (true)

        // Inspect Hive view
        val view_2 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        view_2.provider should be (None)
        view_2.comment should be (None)
        view_2.identifier should be (TableIdentifier("lala", Some("default")))
        view_2.tableType should be (CatalogTableType.VIEW)
        view_2.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", BooleanType) ::
                StructField("partition_col", StringType) ::
                Nil
        ))
        view_2.partitionColumnNames should be (Seq())
        view_2.partitionSchema should be (StructType(Nil))

        // Inspect Hive table
        val table_2 = session.catalog.getTable(TableIdentifier("lala_2", Some("default")))
        table_2.identifier should be (TableIdentifier("lala_2", Some("default")))
        table_2.tableType should be (CatalogTableType.MANAGED)
        table_2.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", BooleanType) ::
                StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))
        table_2.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", BooleanType) ::
                Nil
        ))
        table_2.partitionColumnNames should be (Seq("partition_col"))
        table_2.partitionSchema should be (StructType(
            StructField("partition_col", StringType, nullable = false) ::
                Nil
        ))

        // == Write ===================================================================
        val rdd_2 = spark.sparkContext.parallelize(Seq(
            Row("v2", true)
        ))
        val df_2 = spark.createDataFrame(rdd_2, table_2.dataSchema)
        relation_2.write(executor, df_2, Map("partition_col" -> SingleValue("part_2")))

        // == Read ===================================================================
        val rows_1 = Seq(
            Row("v1", null, "part_1")
        )
        val rows_2 = Seq(
            Row("v2", true, "part_2")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1 ++ rows_2)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), rows_2)

        // == Overwrite ===================================================================
        val rdd_1a = spark.sparkContext.parallelize(Seq(
            Row("v3", false)
        ))
        val df_1a = spark.createDataFrame(rdd_1a, table_2.dataSchema)
        relation_2.write(executor, df_1a, Map("partition_col" -> SingleValue("part_1")))

        // == Read ===================================================================
        val rows_1a = Seq(
            Row("v3", false, "part_1")
        )
        val rows_2a = Seq(
            Row("v2", true, "part_2")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1a ++ rows_2a)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows_1a)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), rows_2a)

        // == Destroy ===================================================================
        relation_2.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_1", Some("default"))) should be (false)
        session.catalog.tableExists(TableIdentifier("lala_2", Some("default"))) should be (false)
    }
}
