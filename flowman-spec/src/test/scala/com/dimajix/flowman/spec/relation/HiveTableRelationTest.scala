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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
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

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class HiveTableRelationTest extends FlatSpec with Matchers with LocalSparkSession with QueryTest {
    "The HiveTableRelation" should "support create" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala_0001", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_0001", Some("default"), Map())))
        relation.fields should be(
            Field("str_col", ftypes.StringType) ::
            Field("int_col", ftypes.IntegerType) ::
            Field("char_col", ftypes.CharType(10)) ::
            Field("varchar_col", ftypes.VarcharType(10)) ::
            Nil)

        relation.create(executor)
        session.catalog.tableExists(TableIdentifier("lala_0001", Some("default"))) should be (true)

        val table = session.catalog.getTable(TableIdentifier("lala_0001", Some("default")))
        table.provider should be (Some("hive"))
        table.comment should be(Some("This is a test table"))
        table.identifier should be (TableIdentifier("lala_0001", Some("default")))
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

        // == Destroy ===================================================================
        relation.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala_0001", Some("default"))) should be (false)

        an[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
    }

    it should "support external tables" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val hiveRelation = relation.asInstanceOf[HiveTableRelation]
        hiveRelation.location should be (Some(new Path(location)))

        relation.create(executor)
        session.catalog.tableExists(TableIdentifier("lala_0002", Some("default"))) should be (true)

        val table = session.catalog.getTable(TableIdentifier("lala_0002", Some("default")))
        table.provider should be (Some("hive"))
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

        // == Destroy ===================================================================
        relation.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala_0002", Some("default"))) should be (false)
    }

    it should "support single partition columns" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala_0003", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_0003", Some("default"), Map())))
        relation.resources(Map("spart" -> SingleValue("x"))) should be (Set(ResourceIdentifier.ofHivePartition("lala_0003", Some("default"), Map("spart" -> "x"))))

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0003", Some("default")))
        table.provider should be (Some("hive"))
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

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support multiple partition columns" in {
        val location = new File(tempDir, "hive/default/lala").toURI
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0004", Some("default")))
        table.provider should be (Some("hive"))
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

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support TBLPROPERTIES" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0005", Some("default")))
        table.provider should be (Some("hive"))
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

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support parquet format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

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
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support avro format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

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
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support csv format" in (if (hiveSupported) {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
              |    database: default
              |    table: lala_0007
              |    format: textfile
              |    rowFormat: org.apache.hadoop.hive.serde2.OpenCSVSerde
              |    serdeProperties:
              |      separatorChar: "\t"
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        relation.create(executor)
        val table = session.catalog.getTable(TableIdentifier("lala_0007", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0007", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        SchemaUtils.dropMetadata(table.schema) should be (StructType(
            StructField("str_col", StringType) ::
            StructField("int_col", StringType) ::
            Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.mapred.TextInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"))
        table.storage.properties should be (Map("separatorChar" -> "\t", "serialization.format" -> "1"))

        // == Destroy ===================================================================
        relation.destroy(executor)
    })

    it should "support a row format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

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
        table.storage.inputFormat should be (Some("org.apache.hadoop.mapred.TextInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))

        // == Destroy ===================================================================
        relation.destroy(executor)
    }

    it should "support input and output format" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
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
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

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

        // == Destroy ===================================================================
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

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)

        if (hiveSupported) {
          spark.catalog.getTable("default", "lala_0010") should not be (null)
          spark.read.table("default.lala_0010").count() should be(0)

          // Test write
          val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
          relation.write(executor, df)
          spark.read.table("default.lala_0010").count() should be(2)

          // Test clean
          relation.truncate(executor)
          location.exists() should be(true)
          spark.catalog.getTable("default", "lala_0010") should not be (null)
          spark.read.table("default.lala_0010").count() should be(0)
        }

        // == Destroy ===================================================================
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

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        if (hiveSupported) {
            spark.catalog.getTable("default", "lala_0011") should not be (null)
            spark.read.table("default.lala_0011").count() should be(0)

            // Test write
            val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
                .withColumnRenamed("_1", "str_col")
                .withColumnRenamed("_2", "int_col")
            relation.write(executor, df)
            spark.read.table("default.lala_0011").count() should be(2)

            // Test clean
            relation.truncate(executor)
            location.exists() should be(true)
            spark.catalog.getTable("default", "lala_0011") should not be (null)
            spark.read.table("default.lala_0011").count() should be(0)
        }

        // == Destroy ===================================================================
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

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // == Create ===================================================================
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        if (hiveSupported) {
            spark.read.table("default.lala_0012").count() should be(0)
        }

        val table = session.catalog.getTable(TableIdentifier("lala_0012", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0012", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("spart", StringType, nullable = false) ::
                StructField("ipart", IntegerType, nullable = false) ::
                Nil
        ))
        table.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq("spart","ipart"))
        table.partitionSchema should be (StructType(
            StructField("spart", StringType, nullable = false) ::
                StructField("ipart", IntegerType, nullable = false) ::
                Nil
        ))

        // == Truncate ===================================================================
        relation.truncate(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        if (hiveSupported) {
            spark.read.table("default.lala_0012").count() should be(0)
        }

        // == Destroy ===================================================================
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))
    }

    it should "support different column orders" in {
        val spec =
            """
              |relations:
              |  t1:
              |    kind: hiveTable
              |    database: default
              |    table: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |        - name: char_col
              |          type: char(10)
              |
              |  t2:
              |    kind: hiveTable
              |    database: default
              |    table: lala
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: char_col
              |          type: char(10)
              |        - name: int_col
              |          type: integer
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation_1 = context.getRelation(RelationIdentifier("t1"))
        relation_1.fields should be(
            Field("str_col", ftypes.StringType) ::
                Field("int_col", ftypes.IntegerType) ::
                Field("char_col", ftypes.CharType(10)) ::
                Nil)

        // == Create ===================================================================
        relation_1.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)

        // Inspect Hive table
        val table = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table.identifier should be (TableIdentifier("lala", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                Nil
        ))
        table.dataSchema should be (StructType(
            StructField("str_col", StringType) ::
                StructField("int_col", IntegerType) ::
                StructField("char_col", StringType) ::
                Nil
        ))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))

        // == Write ===================================================================
        val relation_2 = context.getRelation(RelationIdentifier("t2"))
        val schema = StructType(
            StructField("str_col", StringType) ::
                StructField("char_col", StringType) ::
                StructField("int_col", IntegerType) ::
                Nil
        )
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation_2.write(executor, df, Map())

        // == Read ===================================================================
        val rows_1 = Seq(
            Row("v1", 21, "str")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1)

        // == Destroy ===================================================================
        relation_2.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
    }

    it should "support migration by adding new columns" in {
        val spec =
            """
              |relations:
              |  t1:
              |    kind: hiveTable
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
              |      - name: partition_col
              |        type: string
              |
              |  t2:
              |    kind: hiveTable
              |    database: default
              |    table: lala
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
        relation_1.fields should be(
            Field("str_col", ftypes.StringType) ::
            Field("int_col", ftypes.IntegerType) ::
            Field("partition_col", ftypes.StringType, false) ::
            Nil)

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        relation_1.create(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)

        // Inspect Hive table
        val table_1 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table_1.identifier should be (TableIdentifier("lala", Some("default")))
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

        // Inspect Hive table
        val table_2 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table_2.identifier should be (TableIdentifier("lala", Some("default")))
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
            Row("v1", 21, null, "part_1")
        )
        val rows_2 = Seq(
            Row("v2", 22, "lala", "part_2")
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
            Row("v3", 23, "lala", "part_2")
        )
        checkAnswer(relation_2.read(executor, None, Map()), rows_1 ++ rows_2a)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(executor, None, Map("partition_col" -> SingleValue("part_2"))), rows_2a)

        // == Destroy ===================================================================
        relation_2.destroy(executor)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
    }

    it should "support mapping schemas" in {
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
               |    database: default
               |    table: lala_0004
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
               |mappings:
               |  input:
               |    kind: read
               |    relation: t0
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("input"))

        val schema = mapping.describe(executor, Map(), "main")
        schema should be (ftypes.StructType(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("spart", ftypes.StringType, false),
            Field("ip", ftypes.IntegerType, false)
        )))
    }
}
