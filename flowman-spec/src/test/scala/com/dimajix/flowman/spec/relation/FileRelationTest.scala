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

package com.dimajix.flowman.spec.relation

import java.io.File
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Paths
import java.util.UUID

import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
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
import com.dimajix.spark.sql.streaming.StreamingUtils
import com.dimajix.spark.testing.LocalSparkSession


class FileRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The FileRelation" should "be parseable" in {
        val inputPath = Resources.getResource("data/data_1.csv")
        val spec =
            s"""
              |relations:
              |  t0:
              |    kind: file
              |    location: ${inputPath}
              |    format: csv
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: f1
              |          type: string
              |        - name: f2
              |          type: string
              |        - name: f3
              |          type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.kind should be ("file")

        val fileRelation = relation.asInstanceOf[FileRelation]
        fileRelation.format should be ("csv")
        fileRelation.location should be (new Path(inputPath.toURI))

        val df = relation.read(execution)
        df.schema should be (StructType(
            StructField("f1", StringType) ::
                StructField("f2", StringType) ::
                StructField("f3", StringType) ::
                Nil
        ))
        df.collect()
    }

    it should "be able to create local directories" in {
        val outputPath = Paths.get(tempDir.toString, "csv", "test")
        val spec =
            s"""
               |relations:
               |  local:
               |    kind: file
               |    location: ${outputPath.toUri}
               |    format: csv
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

        val relation = context.getRelation(RelationIdentifier("local"))
        val fileRelation = relation.asInstanceOf[FileRelation]
        fileRelation.location should be (new Path(outputPath.toUri))
        fileRelation.requires should be (Set())
        fileRelation.provides should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.toUri))))
        fileRelation.resources() should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.toUri))))

        // == Create =================================================================================================
        outputPath.toFile.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        outputPath.toFile.exists() should be (true)

        a[FileAlreadyExistsException] shouldBe thrownBy(relation.create(execution))
        relation.create(execution, true)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)

        // == Write ==================================================================================================
        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        outputPath.toFile.exists() should be (true)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (2)

        // == Truncate ===============================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        outputPath.toFile.exists() should be (true)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)

        // == Destroy ================================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        outputPath.toFile.exists() should be (false)

        a[FileNotFoundException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
    }

    it should "work without an explicit schema" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "csv", "test")
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "csv",
            options = Map(
                "inferSchema" -> "true",
                "header" -> "true"
            )
        )

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // == Write ==================================================================================================
        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // == Read ===================================================================================================
        val df1 = relation.read(execution)
        df1.count() should be (2)
        df1.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }

    it should "support partitions" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "csv", "test")
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            pattern = Some("p_col=$p_col"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", com.dimajix.flowman.types.StringType),
                    Field("int_col", com.dimajix.flowman.types.IntegerType)
                )
            )),
            partitions = Seq(
                PartitionField("p_col", com.dimajix.flowman.types.IntegerType)
            )
        )

        relation.requires should be (Set())
        relation.provides should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.toUri))))
        relation.resources() should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.resolve("p_col=*").toUri))))
        relation.resources(Map("p_col" -> SingleValue("22"))) should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.resolve("p_col=22").toUri))))

        // == Inspect ===============================================================================================
        relation.describe(execution) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("str_col", com.dimajix.flowman.types.StringType),
            Field("int_col", com.dimajix.flowman.types.IntegerType),
            Field("p_col", com.dimajix.flowman.types.IntegerType, nullable = false)
        )))
        relation.fields should be (Seq(
            Field("str_col", com.dimajix.flowman.types.StringType),
            Field("int_col", com.dimajix.flowman.types.IntegerType),
            Field("p_col", com.dimajix.flowman.types.IntegerType, nullable = false)
        ))

        // ===== Create =============================================================================================
        outputPath.toFile.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        outputPath.toFile.exists() should be (true)

        // == Inspect ================================================================================================
        relation.read(execution, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
            //StructField("p_col", IntegerType)
        )))
        relation.read(execution, Map("p_col" ->  SingleValue("1"))).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("p_col", IntegerType, nullable=false)
        )))

        // ===== Write =============================================================================================
        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.write(execution, df, Map("p_col" -> SingleValue("2")), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (No)

        // == Inspect ================================================================================================
        relation.read(execution, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
            //StructField("p_col", IntegerType)
        )))
        relation.read(execution, Map("p_col" ->  SingleValue("1"))).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("p_col", IntegerType, nullable=false)
        )))

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (0)

        // ===== Write =============================================================================================
        val df_p1 = relation.read(execution, Map("p_col" -> SingleValue("1")))
        df_p1.count() should be (0)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))
        val df_p2 = relation.read(execution, Map("p_col" -> SingleValue("2")))
        df_p2.count() should be (2)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))

        relation.write(execution, df, Map("p_col" -> SingleValue("3")), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)

        // ===== Truncate =============================================================================================
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)
        relation.truncate(execution, Map("p_col" -> SingleValue("2")))
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)

        // ===== Truncate =============================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (No)
        outputPath.resolve("data.csv").toFile.exists() should be (false)
        outputPath.toFile.exists() should be (true)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (0)

        // ===== Destroy =============================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        outputPath.toFile.exists() should be (false)
    }

    it should "support partitions without explicit pattern" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "csv", "test2")
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", com.dimajix.flowman.types.StringType),
                    Field("int_col", com.dimajix.flowman.types.IntegerType)
                )
            )),
            partitions = Seq(
                PartitionField("p_col", com.dimajix.flowman.types.IntegerType)
            )
        )
        
        relation.requires should be (Set())
        relation.provides should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.toUri))))
        relation.resources() should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.resolve("p_col=*").toUri))))
        relation.resources(Map("p_col" -> SingleValue("22"))) should be (Set(ResourceIdentifier.ofFile(new Path(outputPath.resolve("p_col=22").toUri))))

        // ===== Create =============================================================================================
        outputPath.toFile.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        outputPath.toFile.exists() should be (true)

        // ===== Write =============================================================================================
        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.write(execution, df, Map("p_col" -> SingleValue("2")), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (0)

        // ===== Write =============================================================================================
        val df_p1 = relation.read(execution, Map("p_col" -> SingleValue("1")))
        df_p1.count() should be (0)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))
        val df_p2 = relation.read(execution, Map("p_col" -> SingleValue("2")))
        df_p2.count() should be (2)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))

        relation.write(execution, df, Map("p_col" -> SingleValue("3")), OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)

        // ===== Truncate =============================================================================================
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)
        relation.truncate(execution, Map("p_col" -> SingleValue("2")))
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)

        // ===== Truncate =============================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("3"))) should be (No)
        outputPath.resolve("data.csv").toFile.exists() should be (false)
        outputPath.toFile.exists() should be (true)

        // == Read ===================================================================================================
        relation.read(execution, Map()).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (0)

        // ===== Destroy =============================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)
        outputPath.toFile.exists() should be (false)
    }

    it should "support using wildcards for unspecified partitions" in {
        val spark = this.spark
        import spark.implicits._

        val outputPath = Paths.get(tempDir.toString, "wildcard_partitions_test")
        def mkPartitionFile(p1:String, p2:String, f:String) : Unit = {
            val child = s"p1=$p1/p2=$p2/$f"
            val file = new File(outputPath.toFile, child)
            file.getParentFile.mkdirs()
            file.createNewFile()
            val out = new PrintWriter(file)
            out.println(child)
            out.close()
        }

        mkPartitionFile("1","1","111.txt")
        mkPartitionFile("1","1","112.txt")
        mkPartitionFile("1","2","121.txt")
        mkPartitionFile("1","2","122.txt")
        mkPartitionFile("2","1","211.txt")
        mkPartitionFile("2","1","212.txt")
        mkPartitionFile("2","2","221.txt")
        mkPartitionFile("2","2","222.txt")


        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "text",
            pattern = Some("p1=$p1/p2=$p2"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("value", com.dimajix.flowman.types.StringType)
                )
            )),
            partitions = Seq(
                PartitionField("p1", com.dimajix.flowman.types.IntegerType),
                PartitionField("p2", com.dimajix.flowman.types.IntegerType)
            )
        )


        relation.resources(Map("p1" -> SingleValue("1"), "p2" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "p1=1/p2=1"))
        ))
        relation.resources(Map("p1" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "p1=1/p2=*"))
        ))
        relation.resources(Map("p2" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "p1=*/p2=1"))
        ))
        relation.resources(Map()) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "p1=*/p2=*"))
        ))

        // == Create =================================================================================================
        relation.create(execution, true)
        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)

        // == Read ===================================================================================================
        val df1 = relation.read(execution, Map("p1" -> SingleValue("1"), "p2" -> SingleValue("1")))
        df1.as[(String,Int,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1,1),
            ("p1=1/p2=1/112.txt",1,1)
        ))

        val df2 = relation.read(execution, Map("p1" -> SingleValue("1")))
        df2.as[(String,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1),
            ("p1=1/p2=1/112.txt",1),
            ("p1=1/p2=2/121.txt",1),
            ("p1=1/p2=2/122.txt",1)
        ))

        val df3 = relation.read(execution, Map("p2" -> SingleValue("1")))
        df3.as[(String,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1),
            ("p1=1/p2=1/112.txt",1),
            ("p1=2/p2=1/211.txt",1),
            ("p1=2/p2=1/212.txt",1)
        ))

        val df4 = relation.read(execution, Map())
        df4.as[String].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt"),
            ("p1=1/p2=1/112.txt"),
            ("p1=1/p2=2/121.txt"),
            ("p1=1/p2=2/122.txt"),
            ("p1=2/p2=1/211.txt"),
            ("p1=2/p2=1/212.txt"),
            ("p1=2/p2=2/221.txt"),
            ("p1=2/p2=2/222.txt")
        ))

        // == Truncate ===============================================================================================
        relation.truncate(execution, Map("p2" -> SingleValue("1")))

        // == Read ===================================================================================================
        val df5 = relation.read(execution, Map())
        df5.as[String].collect().sorted should be (Seq(
            ("p1=1/p2=2/121.txt"),
            ("p1=1/p2=2/122.txt"),
            ("p1=2/p2=2/221.txt"),
            ("p1=2/p2=2/222.txt")
        ))

        // == Destroy ================================================================================================
        relation.destroy(execution)
    }

    it should "support using environment variables in pattern" in {
        val spark = this.spark
        import spark.implicits._

        val outputPath = Paths.get(tempDir.toString, "environment_partitions_test")
        def mkPartitionFile(year:String, month:String, f:String) : Unit = {
            val child = s"year=$year/month=$month/$f"
            val file = new File(outputPath.toFile, child)
            file.getParentFile.mkdirs()
            file.createNewFile()
            val out = new PrintWriter(file)
            out.println(child)
            out.close()
        }

        mkPartitionFile("2015","1","111.txt")
        mkPartitionFile("2015","1","112.txt")
        mkPartitionFile("2016","1","111.txt")
        mkPartitionFile("2016","1","112.txt")
        mkPartitionFile("2016","2","121.txt")
        mkPartitionFile("2016","2","122.txt")
        mkPartitionFile("2017","1","211.txt")
        mkPartitionFile("2017","1","212.txt")

        val spec =
            s"""
               |environment:
               |  - year=2016
               |relations:
               |  local:
               |    kind: file
               |    location: ${outputPath.toUri}
               |    pattern: year=$$year/month=$$month
               |    format: text
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: value
               |          type: string
               |    partitions:
               |        - name: month
               |          type: integer
               |""".stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))
        relation.resources(Map("month" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "year=2016/month=1"))
        ))
        relation.resources(Map()) should be (Set(
            ResourceIdentifier.ofFile(new Path(outputPath.toUri.toString, "year=2016/month=*"))
        ))

        relation.create(execution, true)
        relation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)

        val df1 = relation.read(execution, Map("month" -> SingleValue("1")))
        df1.as[(String,Int)].collect().sorted should be (Seq(
            ("year=2016/month=1/111.txt",1),
            ("year=2016/month=1/112.txt",1)
        ))

        val df2 = relation.read(execution, Map())
        df2.as[String].collect().sorted should be (Seq(
            ("year=2016/month=1/111.txt"),
            ("year=2016/month=1/112.txt"),
            ("year=2016/month=2/121.txt"),
            ("year=2016/month=2/122.txt")
        ))

        relation.destroy(execution)
    }

    it should "support different output modes with dynamic partitions" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "csv",
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("c0", com.dimajix.flowman.types.IntegerType),
                    Field("c1", com.dimajix.flowman.types.DoubleType)
                )
            )),
            partitions = Seq(
                PartitionField("part", com.dimajix.flowman.types.StringType)
            )
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
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, Map(), OutputMode.APPEND)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, Map()).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (6)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (0)

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
        relation.read(execution, Map()).count() should be (6)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (1)
        relation.read(execution, Map("part" -> SingleValue("4"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
    }

    it should "support partition columns already present in the schema" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "csv",
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
            )
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
        relation.read(execution, Map()).schema should be (StructType(Seq(
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
        relation.read(execution, Map()).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType)
        )))
        relation.read(execution, Map("part" -> SingleValue("p0"))).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType, nullable=false)
        )))
        relation.read(execution, Map("part" -> SingleValue("p1"))).schema should be (StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", DoubleType),
            StructField("part", StringType, nullable=false)
        )))

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    }

    it should "support different output modes with unpartitioned tables" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val location = Paths.get(tempDir.toString, "some_file_table_123")
        val relation = FileRelation(
            Relation.Properties(context, "rel_1"),
            location = new Path(location.toUri),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.StringType)
                )
            ))
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
        relation.read(execution, Map()).count() should be (4)

        // == Append ================================================================================================
        relation.write(execution, df, Map(), OutputMode.APPEND)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (8)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (4)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map(), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (4)

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.read(execution, Map()).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map(), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (8)

        // == FailIfExists ==========================================================================================
        a[FileAlreadyExistsException] should be thrownBy(relation.write(execution, df, Map(), OutputMode.ERROR_IF_EXISTS))

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.read(execution, Map()).count() should be (0)

        // == FailIfExists ==========================================================================================
        relation.write(execution, df, Map(), OutputMode.ERROR_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (4)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    }

    it should "support different output modes with partitioned tables" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val location = Paths.get(tempDir.toString, "some_file_table_124")
        val relation = FileRelation(
            Relation.Properties(context, "rel_1"),
            location = new Path(location.toUri),
            pattern = Some("part=$part"),
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
            )
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
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Append ================================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.APPEND)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.OVERWRITE)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map("part" -> SingleValue("p0")), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Truncate =============================================================================================
        relation.truncate(execution, Map("part" -> SingleValue("p0")))
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (0)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == IfNotExists ===========================================================================================
        relation.write(execution, df.union(df), Map("part" -> SingleValue("p0")), OutputMode.IGNORE_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == FailIfExists ==========================================================================================
        a[FileAlreadyExistsException] should be thrownBy(relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.ERROR_IF_EXISTS))

        // == Truncate =============================================================================================
        relation.truncate(execution)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (0)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == FailIfExists ==========================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")), OutputMode.ERROR_IF_EXISTS)
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p0"))).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    }

    it should "invalidate any file caches when writing" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val location = Paths.get(tempDir.toString, "some_file_table_456")
        val relation = FileRelation(
            Relation.Properties(context, "rel_1"),
            location = new Path(location.toUri),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("f1", com.dimajix.flowman.types.IntegerType),
                    Field("f2", com.dimajix.flowman.types.DoubleType),
                    Field("f3", com.dimajix.flowman.types.StringType)
                )
            ))
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
        val df2 = relation.read(execution, Map())
        df2.count() should be (4)

        // == Overwrite =============================================================================================
        relation.write(execution, df, Map(), OutputMode.OVERWRITE)

        // == Read ==================================================================================================
        df2.count() should be (4)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    }

    it should "support mapping schemas" in {
        val outputPath = Paths.get(tempDir.toString, "csv", "test")
        val spec =
            s"""
               |relations:
               |  local:
               |    kind: file
               |    location: ${outputPath.toUri}
               |    pattern: p_col=$$p_col
               |    format: csv
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: str_col
               |          type: string
               |        - name: int_col
               |          type: integer
               |    partitions:
               |        - name: p_col
               |          type: integer
               |mappings:
               |  input:
               |    kind: read
               |    relation: local
               |    partitions:
               |      spart: abc
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
            Field("p_col", ftypes.IntegerType, false)
        )))
    }

    it should "support stream reading" in {
        val outputPath = Paths.get(tempDir.toString, "streaming_test_1")
        def mkFile(f:String) : Unit = {
            val child = s"$f"
            val file = new File(outputPath.toFile, child)
            file.getParentFile.mkdirs()
            file.createNewFile()
            val out = new PrintWriter(file)
            out.println(child)
            out.close()
        }

        mkFile("111.txt")
        mkFile("112.txt")
        mkFile("121.txt")
        mkFile("122.txt")
        mkFile("211.txt")
        mkFile("212.txt")
        mkFile("221.txt")
        mkFile("222.txt")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "text",
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("value", com.dimajix.flowman.types.StringType)
                )
            ))
        )

        // == Read ==================================================================================================
        def verify(df:Dataset[Row]) : Unit = {
            df.count() should be (8)
        }
        val df = relation.readStream(execution)
        val query = df.writeStream
            .trigger(Trigger.Once())
            .foreachBatch((df:Dataset[Row], _:Long) => verify(df))
            .start()

        query.awaitTermination()

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }

    it should "support stream reading with partitions" in {
        val outputPath = Paths.get(tempDir.toString, "streaming_test_2")
        def mkPartitionFile(p1:String, p2:String, f:String) : Unit = {
            val child = s"magic_p1=$p1/$p2/$f"
            val file = new File(outputPath.toFile, child)
            file.getParentFile.mkdirs()
            file.createNewFile()
            val out = new PrintWriter(file)
            out.println(child)
            out.close()
        }

        mkPartitionFile("1","1","111.txt")
        mkPartitionFile("1","1","112.txt")
        mkPartitionFile("1","2","121.txt")
        mkPartitionFile("1","2","122.txt")
        mkPartitionFile("2","1","211.txt")
        mkPartitionFile("2","1","212.txt")
        mkPartitionFile("2","2","221.txt")
        mkPartitionFile("2","2","222.txt")


        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "text",
            pattern = Some("magic_p1=$p1/$p2"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("value", com.dimajix.flowman.types.StringType)
                )
            )),
            partitions = Seq(
                PartitionField("p1", com.dimajix.flowman.types.IntegerType),
                PartitionField("p2", com.dimajix.flowman.types.IntegerType)
            )
        )

        // == Read ==================================================================================================
        def verify(df:Dataset[Row]) : Unit = {
            df.count() should be (8)
        }
        val df = relation.readStream(execution)
        val query = df.writeStream
            .trigger(Trigger.Once())
            .foreachBatch((df:Dataset[Row], _:Long) => verify(df))
            .start()

        query.awaitTermination()

        // == Destroy ===============================================================================================
        relation.destroy(execution)
    }

    it should "support stream writing" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "streaming_test_" + UUID.randomUUID().toString)
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "csv",
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("c0", com.dimajix.flowman.types.IntegerType),
                    Field("c1", com.dimajix.flowman.types.DoubleType),
                    Field("c2", com.dimajix.flowman.types.StringType)
                )
            ))
        )

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution) should be (No)

        // == Write ==================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, null),
            Row(234, 123.0, ""),
            Row(2345, 1234.0, "1234567"),
            Row(23456, 12345.0, "1234567")
        ))
        val df0 = spark.createDataFrame(rdd, relation.schema.get.catalogSchema)
        val df1 = StreamingUtils.createSingleTriggerStreamingDF(df0)

        val checkpoint = Paths.get(tempDir.toString, "streaming_checkpoint_" + UUID.randomUUID().toString).toUri
        val query1 = relation.writeStream(execution, df1, OutputMode.APPEND, Trigger.Once(), new Path(checkpoint))
        query1.awaitTermination()

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (4)

        // == Write ==================================================================================================
        val df2 = StreamingUtils.createSingleTriggerStreamingDF(df0, 1)
        val query2 = relation.writeStream(execution, df2, OutputMode.APPEND, Trigger.Once(), new Path(checkpoint))
        query2.awaitTermination()

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.read(execution, Map()).count() should be (8)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
    }

    it should "support stream writing with partitions" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context

        val outputPath = Paths.get(tempDir.toString, "streaming_test_" + UUID.randomUUID().toString)
        val relation = FileRelation(
            Relation.Properties(context, "local"),
            location = new Path(outputPath.toUri),
            format = "csv",
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("c0", com.dimajix.flowman.types.IntegerType),
                    Field("c1", com.dimajix.flowman.types.DoubleType)
                )
            )),
            partitions = Seq(
                PartitionField("part", com.dimajix.flowman.types.StringType)
            )
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
        val df0 = spark.createDataFrame(rdd, StructType(relation.fields.map(_.catalogField)))
        val df1 = StreamingUtils.createSingleTriggerStreamingDF(df0)

        val checkpoint = Paths.get(tempDir.toString, "streaming_checkpoint_" + UUID.randomUUID().toString).toUri
        val query1 = relation.writeStream(execution, df1, OutputMode.APPEND, Trigger.Once(), new Path(checkpoint))
        query1.awaitTermination()

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, Map()).count() should be (4)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (3)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Write ==================================================================================================
        val df2 = StreamingUtils.createSingleTriggerStreamingDF(df0, 1)
        val query2 = relation.writeStream(execution, df2, OutputMode.APPEND, Trigger.Once(), new Path(checkpoint))
        query2.awaitTermination()

        // == Read ==================================================================================================
        relation.loaded(execution) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
        relation.read(execution, Map()).count() should be (8)
        relation.read(execution, Map("part" -> SingleValue("1"))).count() should be (6)
        relation.read(execution, Map("part" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("part" -> SingleValue("3"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("1"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("2"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("3"))) should be (No)
    }
}
