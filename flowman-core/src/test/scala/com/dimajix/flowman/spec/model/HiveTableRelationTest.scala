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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class HiveTableRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The HiveTableRelation" should "support create" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    description: "This is a test table"
              |    database: default
              |    table: lala_0001
              |    external: false
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0001", Some("default")))
        table.comment should be(Some("This is a test table"))
        table.identifier should be (TableIdentifier("lala_0001", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location.toString should not be ("")

        // Try to create relation, although it already exists
        a[TableAlreadyExistsException] shouldBe thrownBy(relation.create(executor))
        relation.create(executor, true)

        // Drop table
        relation.destroy(executor)
        an[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
    }

    it should "support external tables" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: table
               |    database: default
               |    table: lala_0002
               |    external: true
               |    location: $location
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
        implicit val context = executor.context
        val relation = project.relations("t0")

        val hiveRelation = relation.asInstanceOf[HiveTableRelation]
        hiveRelation.location should be (new Path(location))

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0002", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0002", Some("default")))
        table.tableType should be (CatalogTableType.EXTERNAL)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should be (location)
        relation.destroy(executor)
    }

    it should "support single partition columns" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: table
               |    database: default
               |    table: lala_0003
               |    external: false
               |    location: $location
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0003", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0003", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("spart", StringType, false) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq("spart"))
        table.partitionSchema should be (StructType(
            StructField("spart", StringType, false) ::
                Nil
        ))
        table.location should be (location)
        relation.destroy(executor)
    }

    it should "support multiple partition columns" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: table
               |    database: default
               |    table: lala_0004
               |    location: $location
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0004", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0004", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("spart", StringType, false) ::
                StructField("ip", IntegerType, false) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq("spart", "ip"))
        table.partitionSchema should be (StructType(
            StructField("spart", StringType, false) ::
                StructField("ip", IntegerType, false) ::
                Nil
        ))
        table.location should be (location)
        relation.destroy(executor)
    }

    it should "support TBLPROPERTIES" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala_0005
              |    properties:
              |      lala: lolo
              |      hive.property: TRUE
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0005", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0005", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location.toString should not be ("")
        table.properties("lala") should be ("lolo")
        table.properties("hive.property") should be ("TRUE")
        relation.destroy(executor)
    }

    it should "support parquet format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala_0006
              |    format: parquet
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0006", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0006", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("parquet"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
        relation.destroy(executor)
    }

    it should "support avro format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala_0007
              |    format: avro
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0007", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0007", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("avro"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
        relation.destroy(executor)
    }

    it should "support a row format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala_0008
              |    rowFormat: org.apache.hadoop.hive.serde2.avro.AvroSerDe
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0008", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0008", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.mapred.SequenceFileInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
        relation.destroy(executor)
    }

    it should "support input and output format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: table
              |    database: default
              |    table: lala_0009
              |    rowFormat: org.apache.hadoop.hive.serde2.avro.AvroSerDe
              |    inputFormat: org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat
              |    outputFormat: org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat
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
        val relation = project.relations("t0")

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0009", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0009", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
        relation.destroy(executor)
    }


    it should "support create, clean and destroy without partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
               |    database: default
               |    table: lala_0010
               |    external: false
               |    location: ${location.toURI}
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: str_col
               |          type: string
               |        - name: int_col
               |          type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0010") should not be (null)
        spark.read.table("default.lala_0010").count() should be (0)

        // Test write
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(executor, df)
        spark.read.table("default.lala_0010").count() should be (2)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0010") should not be (null)
        spark.read.table("default.lala_0010").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))
    }

    it should "support external tables for create, clean and destroy without partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
               |    database: default
               |    table: lala_0011
               |    external: true
               |    location: ${location.toURI}
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: str_col
               |          type: string
               |        - name: int_col
               |          type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0011") should not be (null)
        spark.read.table("default.lala_0011").count() should be (0)

        // Test write
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(executor, df)
        spark.read.table("default.lala_0011").count() should be (2)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0011") should not be (null)
        spark.read.table("default.lala_0011").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))
    }

    it should "support create, clean and with partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
               |    database: default
               |    table: lala_0012
               |    external: false
               |    location: ${location.toURI}
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
               |      - name: ipart
               |        type: integer
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        spark.read.table("default.lala_0012").count() should be (0)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        spark.read.table("default.lala_0012").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))
    }
}
