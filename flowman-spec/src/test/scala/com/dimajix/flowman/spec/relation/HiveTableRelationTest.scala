/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.features.hiveVarcharSupported
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class HiveTableRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession with QueryTest {
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
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala_0001", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_0001", Some("default"), Map())))
        relation.fields should be(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType)
        ))

        // == Create ===================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        session.catalog.tableExists(TableIdentifier("lala_0001", Some("default"))) should be (true)

        val table = session.catalog.getTable(TableIdentifier("lala_0001", Some("default")))
        table.provider should be (Some("hive"))
        table.comment should be(Some("This is a test table"))
        table.identifier should be (TableIdentifier("lala_0001", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location.toString should not be ("")

        // Try to create relation, although it already exists
        a[TableAlreadyExistsException] shouldBe thrownBy(relation.create(execution))
        relation.create(execution, true)

        // == Truncate ===================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // == Destroy ===================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        session.catalog.tableExists(TableIdentifier("lala_0001", Some("default"))) should be (false)

        an[NoSuchTableException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
    }

    it should "support external tables" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hiveTable
               |    database: default
               |    table: lala_0002
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val hiveRelation = relation.asInstanceOf[HiveTableRelation]
        hiveRelation.location should be (Some(new Path(location.toURI)))

        // == Create ===================================================================
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        session.catalog.tableExists(TableIdentifier("lala_0002", Some("default"))) should be (true)

        val table = session.catalog.getTable(TableIdentifier("lala_0002", Some("default")))
        table.provider should be (Some("hive"))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0002", Some("default")))
        table.tableType should be (CatalogTableType.EXTERNAL)
        table.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        new File(table.location) should be (location)

        // == Destroy ===================================================================
        relation.destroy(execution)
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.provides should be (Set(ResourceIdentifier.ofHiveTable("lala_0003", Some("default"))))
        relation.requires should be (Set(ResourceIdentifier.ofHiveDatabase("default")))
        relation.resources() should be (Set(ResourceIdentifier.ofHivePartition("lala_0003", Some("default"), Map())))
        relation.resources(Map("spart" -> SingleValue("x"))) should be (Set(ResourceIdentifier.ofHivePartition("lala_0003", Some("default"), Map("spart" -> "x"))))

        // == Create ===================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"))) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("1"))) should be (No)

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
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"))) should be (No)
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        // == Create ===================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ip" -> SingleValue("2"))) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ip" -> SingleValue("2"))) should be (No)

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
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ip" -> SingleValue("2"))) should be (No)
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ip" -> SingleValue("2"))) should be (No)
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
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

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }

    it should "support parquet format" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            database = Some("default"),
            table = "lala_0006",
            format = Some("parquet"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
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

        // == Destroy ================================================================================================
        relation.destroy(execution)
    }

    it should "support avro format" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            database = Some("default"),
            table = "lala_0007",
            format = Some("avro"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
        val table = session.catalog.getTable(TableIdentifier("lala_0007", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0007", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }

    it should "support csv format" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            database = Some("default"),
            table = "lala_0007",
            format = Some("textfile"),
            rowFormat = Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
            serdeProperties = Map(
                "respectSparkSchema" -> "true", // If not set, Spark will return StringType
                "separatorChar" -> "\t"
            ),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
        val table = session.catalog.getTable(TableIdentifier("lala_0007", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0007", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        SchemaUtils.dropMetadata(table.schema) should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.mapred.TextInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"))
        table.storage.properties should be (Map("separatorChar" -> "\t", "respectSparkSchema" -> "true", "serialization.format" -> "1"))

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    })

    it should "support a row format" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            database = Some("default"),
            table = "lala_0008",
            rowFormat = Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
        val table = session.catalog.getTable(TableIdentifier("lala_0008", Some("default")))
        table.comment should be(None)
        table.identifier should be (TableIdentifier("lala_0008", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))
        table.location should not be (None)
        table.provider should be (Some("hive"))
        table.storage.inputFormat should be (Some("org.apache.hadoop.mapred.TextInputFormat"))
        table.storage.outputFormat should be (Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
        table.storage.serde should be (Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }

    it should "support input and output format" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            database = Some("default"),
            table = "lala_0009",
            rowFormat = Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"),
            inputFormat = Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"),
            outputFormat = Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )

        // == Create ================================================================================================
        relation.create(execution)

        // == Check =================================================================================================
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

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }


    it should "support create, clean and destroy without partitions" in { if (hiveSupported) {
        val location = new File(tempDir, "hive/default/lala601")
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))

        // == Create ===================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0010") should not be (null)

        // == Read ===================================================================
        spark.read.table("default.lala_0010").count() should be(0)
        relation.read(execution, None).count() should be (0)

        // == Write ===================================================================
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.write(execution, df)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // == Read ===================================================================
        spark.read.table("default.lala_0010").count() should be(2)
        relation.read(execution, None).count() should be (2)

        // == Overwrite ===================================================================
        relation.write(execution, df)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // == Read ===================================================================
        spark.read.table("default.lala_0010").count() should be(2)
        relation.read(execution, None).count() should be (2)

        // == Truncate ===================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        location.exists() should be(true)
        spark.catalog.getTable("default", "lala_0010") should not be (null)

        // == Read ===================================================================
        spark.read.table("default.lala_0010").count() should be(0)
        relation.read(execution, None).count() should be (0)

        // == Destroy ===================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0010"))
    }}

    it should "support external tables for create, clean and destroy without partitions" in { if (hiveSupported) {
        val location = new File(tempDir, "hive/default/lala677")
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))

        // == Create ===================================================================
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0011") should not be (null)

        // == Read ===================================================================
        spark.read.table("default.lala_0011").count() should be(0)
        relation.read(execution, None).count() should be (0)

        // == Write ===================================================================
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(execution, df)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // == Read ===================================================================
        spark.read.table("default.lala_0011").count() should be(2)
        relation.read(execution, None).count() should be (2)

        // == Truncate ===================================================================
        relation.truncate(execution)
        location.exists() should be(true)
        relation.loaded(execution, Map()) should be (No)
        spark.catalog.getTable("default", "lala_0011") should not be (null)

        // == Read ===================================================================
        spark.read.table("default.lala_0011").count() should be(0)
        relation.read(execution, None).count() should be (0)

        // == Destroy ===================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0011"))
    }}

    it should "support create, clean and destroy with partitions" in { if (hiveSupported) {
        val location = new File(tempDir, "hive/default/lala744")
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
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // == Create ================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ipart" -> SingleValue("2"))) should be (No)
        relation.create(execution)

        // == Check =================================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ipart" -> SingleValue("2"))) should be (No)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        spark.read.table("default.lala_0012").count() should be(0)

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

        // == Write ==================================================================================================
        val schema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("char_col", StringType),
            StructField("int_col", IntegerType)
        ))
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation.write(execution, df, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))).count() should be (0)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution, None, Map()).count() should be (2)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))).count() should be (1)

        // == Truncate ==============================================================================================
        relation.truncate(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23")))
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"))) should be (Yes)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))) should be (Yes)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        spark.read.table("default.lala_0012").count() should be(1)

        // == Read ===================================================================================================
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"))).count() should be (1)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))).count() should be (1)

        // == Truncate ===============================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))) should be (No)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala_0012") should not be (null)
        spark.read.table("default.lala_0012").count() should be(0)

        // == Read ===================================================================================================
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("23"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p0"), "ipart" -> SingleValue("24"))).count() should be (0)
        relation.read(execution, None, Map("spart" -> SingleValue("p1"), "ipart" -> SingleValue("23"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("spart" -> SingleValue("1"), "ipart" -> SingleValue("2"))) should be (No)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))

        // Test 2nd destruction
        a[NoSuchTableException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala_0012"))
    }}

    it should "support different output modes with unpartitioned tables" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.StringType)
                )
            )),
            table = "some_table",
            database = Some("default")
        )

        // == Create ================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution) should be (No)

        // == Write =================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, null),
            Row(234, 123.0, ""),
            Row(2345, 1234.0, "1234567"),
            Row(23456, 12345.0, "1234567")
        ))
        val df = spark.createDataFrame(rdd, relation.schema.get.catalogSchema)
        relation.write(execution, df, Map())

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (4)

        // == Append ================================================================================================
        relation.write(execution, df, Map(), OutputMode.APPEND)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (8)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (4)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map(), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (4)

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.read(execution, None, Map()).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map(), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (8)

        // == FailIfExists ==========================================================================================
        a[TableAlreadyExistsException] should be thrownBy(relation.write(execution, df, Map(), OutputMode.ERROR_IF_EXISTS))

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.read(execution, None, Map()).count() should be (0)

        // == FailIfExists ==========================================================================================
        relation.write(execution, df, Map(), OutputMode.ERROR_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, None, Map()).count() should be (4)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    })

    it should "support different output modes with partitioned tables" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.StringType)
                )
            )),
            partitions = Seq(
                PartitionField("part", com.dimajix.flowman.types.StringType)
            ),
            table = "some_table",
            database = Some("default")
        )

        // == Create ================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Write =================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, null),
            Row(234, 123.0, ""),
            Row(2345, 1234.0, "1234567"),
            Row(23456, 12345.0, "1234567")
        ))
        val df = spark.createDataFrame(rdd, relation.schema.get.catalogSchema)
        relation.write(execution, df, Map("part" -> SingleValue("p0")))

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Append ================================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.APPEND)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (8)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (8)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.OVERWRITE)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map("part" -> SingleValue("p0")), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Truncate =============================================================================================
        relation.truncate(execution, Map("part" -> SingleValue("p0")))
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map("part" -> SingleValue("p0")), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (8)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (8)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == FailIfExists ==========================================================================================
        a[PartitionAlreadyExistsException] should be thrownBy(relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.ERROR_IF_EXISTS))

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == FailIfExists ==========================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.ERROR_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    })

    it should "support different output modes with dynamic partitions" in (if (hiveSupported) {
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig("hive.exec.dynamic.partition.mode", "nonstrict")
            .build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType)
                )
            )),
            partitions = Seq(
                PartitionField("part", com.dimajix.flowman.types.StringType)
            ),
            table = "some_table",
            database = Some("default")
        )

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution) should be (No)

        // == Write ==================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, "1"),
            Row(234, 123.0, "1"),
            Row(2345, 1234.0, "1"),
            Row(23456, 12345.0, "2")
        ))
        val df = spark.createDataFrame(rdd, StructType(relation.fields.map(_.catalogField)))
        relation.write(execution, df, Map(), OutputMode.APPEND)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, None, Map("part" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, Map(), OutputMode.APPEND)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, None, Map()).count() should be (8)
        relation.read(execution, None, Map("part" -> SingleValue("1"))).count() should be (6)
        relation.read(execution, None, Map("part" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, None, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Overwrite ==============================================================================================
        val rdd2 = spark.sparkContext.parallelize(Seq(
            Row(null, null, "1"),
            Row(234, 123.0, "1"),
            Row(2345, 1234.0, "1"),
            Row(23456, 12345.0, "3")
        ))
        val df2 = spark.createDataFrame(rdd2, StructType(relation.fields.map(_.catalogField)))
        relation.write(execution, df2, Map(), OutputMode.OVERWRITE_DYNAMIC)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("4"))) should be (No)
        relation.read(execution, None, Map()).count() should be (6)
        relation.read(execution, None, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, None, Map("part" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, None, Map("part" -> SingleValue("3"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("4"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, None, Map("part" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
    })

    it should "support partition columns already present in the schema" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("part", com.dimajix.flowman.types.StringType)
                )
            )),
            partitions = Seq(
                PartitionField("part", com.dimajix.flowman.types.StringType)
            ),
            table = "some_table",
            database = Some("default")
        )

        // == Inspect ===============================================================================================
        relation.describe(execution) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("f1", com.dimajix.flowman.types.IntegerType),
            Field("f2", com.dimajix.flowman.types.DoubleType),
            Field("part", com.dimajix.flowman.types.StringType, nullable = false)
        )))
        relation.fields should be (Seq(
            Field("f1", com.dimajix.flowman.types.IntegerType),
            Field("f2", com.dimajix.flowman.types.DoubleType),
            Field("part", com.dimajix.flowman.types.StringType, nullable = false)
        ))

        // == Create ================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)

        // == Inspect ================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))
        relation.read(execution, None, Map("part" ->  SingleValue("p0"))).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))

        // == Write =================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null),
            Row(234, 123.0),
            Row(2345, 1234.0),
            Row(23456, 12345.0)
        ))
        val partitionSchema = StructType(relation.schema.get.fields.map(_.catalogField).dropRight(1))
        val df = spark.createDataFrame(rdd, partitionSchema)
        relation.write(execution, df, Map("part" -> SingleValue("p0")))

        // == Inspect ================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, None, Map()).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    })

    it should "support different column orders" in (if (hiveSupported) {
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
              |          type: string
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
              |          type: string
              |        - name: int_col
              |          type: integer
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation_1 = context.getRelation(RelationIdentifier("t1"))
        relation_1.fields should be(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("char_col", ftypes.StringType)
        ))

        // == Create ===================================================================
        relation_1.create(execution)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)

        // Inspect Hive table
        val table = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table.identifier should be (TableIdentifier("lala", Some("default")))
        table.tableType should be (CatalogTableType.MANAGED)
        table.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("char_col", StringType)
        )))
        table.dataSchema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("char_col", StringType)
        )))
        table.partitionColumnNames should be (Seq())
        table.partitionSchema should be (StructType(Nil))

        // == Write ===================================================================
        val relation_2 = context.getRelation(RelationIdentifier("t2"))
        val schema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("char_col", StringType),
            StructField("int_col", IntegerType)
        ))
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation_2.write(execution, df, Map())

        // == Read ===================================================================
        val rows_1 = Seq(
            Row("v1", 21, "str")
        )
        checkAnswer(relation_2.read(execution, None, Map()), rows_1)

        // == Destroy ===================================================================
        relation_2.destroy(execution)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
    })

    it should "support char/varchar columns" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.VarcharType(4)),
                    Field("f2", com.dimajix.flowman.types.CharType(4)),
                    Field("f3", com.dimajix.flowman.types.StringType)
                )
            )),
            table = "some_table",
            database = Some("default")
        )

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (false)
        relation.create(execution)
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (true)

        // Inspect Hive table
        val table_1 = session.catalog.getTable(TableIdentifier("some_table", Some("default")))
        table_1.identifier should be (TableIdentifier("some_table", Some("default")))
        table_1.tableType should be (CatalogTableType.MANAGED)
        if (hiveVarcharSupported) {
            table_1.schema should be(StructType(Seq(
                StructField("f1", VarcharType(4)),
                StructField("f2", CharType(4)),
                StructField("f3", StringType)
            )))
            table_1.dataSchema should be(StructType(Seq(
                StructField("f1", VarcharType(4)),
                StructField("f2", CharType(4)),
                StructField("f3", StringType)
            )))
        }
        else {
            table_1.schema should be(StructType(Seq(
                StructField("f1", StringType),
                StructField("f2", StringType),
                StructField("f3", StringType)
            )))
            table_1.dataSchema should be(StructType(Seq(
                StructField("f1", StringType),
                StructField("f2", StringType),
                StructField("f3", StringType)
            )))
        }
        table_1.partitionColumnNames should be (Seq())
        table_1.partitionSchema should be (StructType(Seq()))

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, null),
            Row("234", "123", ""),
            Row("2345", "1234", "1234567"),
            Row("23456", "12345", "1234567")
        ))
        val df = spark.createDataFrame(rdd, SchemaUtils.replaceCharVarchar(table_1.dataSchema))
        relation.write(execution, df, Map())

        // == Read ===================================================================
        val rows = Seq(
            Row(null, null, null),
            Row("234", "123 ", ""),
            Row("2345", "1234", "1234567"),
            Row("2345", "1234", "1234567")
        )
        checkAnswer(relation.read(execution, None, Map()), rows)

        // == Destroy ===================================================================
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (true)
        relation.destroy(execution)
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (false)
    })

    it should "support migration by adding new columns" in (if (hiveSupported) {
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
              |          type: varchar(10)
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: partition_col
              |        type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation_1 = context.getRelation(RelationIdentifier("t1"))
        relation_1.fields should be(
            Field("str_col", ftypes.StringType) ::
            Field("int_col", ftypes.IntegerType) ::
            Field("partition_col", ftypes.StringType, false) ::
            Nil)

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
        relation_1.create(execution)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (true)

        // Inspect Hive table
        val table_1 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table_1.identifier should be (TableIdentifier("lala", Some("default")))
        table_1.tableType should be (CatalogTableType.MANAGED)
        table_1.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("partition_col", StringType, nullable = false)
        )))
        table_1.dataSchema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))
        table_1.partitionColumnNames should be (Seq("partition_col"))
        table_1.partitionSchema should be (StructType(Seq(
            StructField("partition_col", StringType, nullable = false)
        )))

        // == Write ===================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", 21)
        ))
        val df = spark.createDataFrame(rdd, table_1.dataSchema)
        relation_1.write(execution, df, Map("partition_col" -> SingleValue("part_1")))

        // == Migrate ===================================================================
        val relation_2 = context.getRelation(RelationIdentifier("t2"))
        relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)

        // Inspect Hive table
        val table_2 = session.catalog.getTable(TableIdentifier("lala", Some("default")))
        table_2.identifier should be (TableIdentifier("lala", Some("default")))
        table_2.tableType should be (CatalogTableType.MANAGED)
        if (hiveVarcharSupported) {
            table_2.schema should be(StructType(Seq(
                StructField("str_col", StringType),
                StructField("int_col", IntegerType),
                StructField("char_col", VarcharType(10)),
                StructField("partition_col", StringType, nullable = false)
            )))
            table_2.dataSchema should be(StructType(Seq(
                StructField("str_col", StringType),
                StructField("int_col", IntegerType),
                StructField("char_col", VarcharType(10))
            )))
        }
        else {
            table_2.schema should be(StructType(Seq(
                StructField("str_col", StringType),
                StructField("int_col", IntegerType),
                StructField("char_col", StringType),
                StructField("partition_col", StringType, nullable = false)
            )))
            table_2.dataSchema should be(StructType(Seq(
                StructField("str_col", StringType),
                StructField("int_col", IntegerType),
                StructField("char_col", StringType)
            )))
        }
        table_2.partitionColumnNames should be (Seq("partition_col"))
        table_2.partitionSchema should be (StructType(Seq(
            StructField("partition_col", StringType, nullable = false)
        )))

        // == Write ===================================================================
        val rdd_2 = spark.sparkContext.parallelize(Seq(
            Row("v2", 22, "lala")
        ))
        val df2 = spark.createDataFrame(rdd_2, SchemaUtils.replaceCharVarchar(table_2.dataSchema))
        relation_2.write(execution, df2, Map("partition_col" -> SingleValue("part_2")))

        // == Read ===================================================================
        val rows_1 = Seq(
            Row("v1", 21, null, "part_1")
        )
        val rows_2 = Seq(
            Row("v2", 22, "lala", "part_2")
        )
        checkAnswer(relation_2.read(execution, None, Map()), rows_1 ++ rows_2)
        checkAnswer(relation_2.read(execution, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(execution, None, Map("partition_col" -> SingleValue("part_2"))), rows_2)

        // == Overwrite ===================================================================
        val rdd_2a = spark.sparkContext.parallelize(Seq(
            Row("v3", 23, "lala")
        ))
        val df2a = spark.createDataFrame(rdd_2a, SchemaUtils.replaceCharVarchar(table_2.dataSchema))
        relation_2.write(execution, df2a, Map("partition_col" -> SingleValue("part_2")))

        // == Read ===================================================================
        val rows_2a = Seq(
            Row("v3", 23, "lala", "part_2")
        )
        checkAnswer(relation_2.read(execution, None, Map()), rows_1 ++ rows_2a)
        checkAnswer(relation_2.read(execution, None, Map("partition_col" -> SingleValue("part_1"))), rows_1)
        checkAnswer(relation_2.read(execution, None, Map("partition_col" -> SingleValue("part_2"))), rows_2a)

        // == Destroy ===================================================================
        relation_2.destroy(execution)
        session.catalog.tableExists(TableIdentifier("lala", Some("default"))) should be (false)
    })

    it should "support relaxed migrations" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation_1 = HiveTableRelation(
            Relation.Properties(context, "rel_1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.IntegerType),
                    Field("f3", com.dimajix.flowman.types.IntegerType)
                )
            )),
            table = "some_table",
            database = Some("default")
        )
        val relation_2 = HiveTableRelation(
            Relation.Properties(context, "rel_2"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.StringType),
                    Field("f2", com.dimajix.flowman.types.ShortType),
                    Field("f4", com.dimajix.flowman.types.LongType)
                )
            )),
            table = "some_table",
            database = Some("default")
        )

        // == Create ===================================================================
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (false)
        relation_1.create(execution)
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (true)
        session.catalog.getTable(relation_1.tableIdentifier).schema should be (StructType(Seq(
            StructField("f1", StringType),
            StructField("f2", IntegerType),
            StructField("f3", IntegerType)
        )))

        // == Migrate ===================================================================
        relation_1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
        relation_1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL)
        relation_1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
        relation_1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
        relation_1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.REPLACE)
        session.catalog.getTable(relation_1.tableIdentifier).schema should be (StructType(Seq(
            StructField("f1", StringType),
            StructField("f2", IntegerType),
            StructField("f3", IntegerType)
        )))

        // == Migrate ===================================================================
        relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
        a[MigrationFailedException] should be thrownBy(relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL))
        relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
        //relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
        //relation_2.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.REPLACE)
        session.catalog.getTable(relation_2.tableIdentifier).schema should be (StructType(Seq(
            StructField("f1", StringType),
            StructField("f2", IntegerType),
            StructField("f3", IntegerType),
            StructField("f4", LongType)
        )))

        // == Destroy ===================================================================
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (true)
        relation_1.destroy(execution)
        session.catalog.tableExists(TableIdentifier("some_table", Some("default"))) should be (false)
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
        val execution = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("input"))

        val schema = mapping.describe(execution, Map(), "main")
        schema should be (ftypes.StructType(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("spart", ftypes.StringType, false),
            Field("ip", ftypes.IntegerType, false)
        )))
    }

    it should "replace an existing Hive view with a Hive table" in {
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

        // == Create VIEW ============================================================================================
        view.exists(execution) should be (No)
        view.create(execution)
        view.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.VIEW)

        table.exists(execution) should be (Yes)
        view.exists(execution) should be (Yes)

        // == Create TABLE ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (table.create(execution, ifNotExists = false))
        table.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.VIEW)

        // == Create VIEW ============================================================================================
        a[TableAlreadyExistsException] should be thrownBy (view.create(execution, ifNotExists = false))
        view.create(execution, ifNotExists = true)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.VIEW)

        // == Migrate TABLE ==========================================================================================
        table.migrate(execution)
        view.exists(execution) should be (Yes)
        table.exists(execution) should be (Yes)
        session.catalog.getTable(TableIdentifier("table_or_view", Some("default"))).tableType should be (CatalogTableType.MANAGED)

        // == Destroy TABLE ===========================================================================================
        table.destroy(execution)
        view.exists(execution) should be (No)
        table.exists(execution) should be (No)
        session.catalog.tableExists(TableIdentifier("table_or_view", Some("default"))) should be (false)

        // == Destroy VIEW ===========================================================================================
        view.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(view.destroy(execution, ifExists = false))

        // == Destroy TABLE ==========================================================================================
        table.destroy(execution, ifExists = true)
        a[NoSuchTableException] should be thrownBy(table.destroy(execution, ifExists = false))

        context.getRelation(RelationIdentifier("t0")).destroy(execution)
    }
}
