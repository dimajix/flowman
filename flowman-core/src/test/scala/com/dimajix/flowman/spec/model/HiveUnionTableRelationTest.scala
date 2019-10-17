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
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala", Some("default")), ResourceIdentifier.ofHiveTable("lala_*", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_*", Some("default"), Map())))
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

    it should "support migration by adding new columns" in {

    }

    it should "support migration by creating new tables" in {

    }

    it should "support partitions" in {

    }
}
