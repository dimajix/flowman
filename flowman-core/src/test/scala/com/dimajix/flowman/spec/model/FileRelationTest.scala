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

package com.dimajix.flowman.spec.model

import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.{types => ftypes}


class FileRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The FileRelation" should "be parseable" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: file
              |    location: test/data/data_1.csv
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
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.kind should be ("file")

        val fileRelation = relation.asInstanceOf[FileRelation]
        fileRelation.format should be ("csv")
        fileRelation.location should be (new Path("test/data/data_1.csv"))

        val df = relation.read(executor, None)
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
               |    pattern: data.csv
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
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))
        val fileRelation = relation.asInstanceOf[FileRelation]
        fileRelation.location should be (new Path(outputPath.toUri))

        outputPath.toFile.exists() should be (false)
        relation.create(executor)
        outputPath.toFile.exists() should be (true)
        outputPath.resolve("data.csv").toFile.exists() should be (false)

        a[FileAlreadyExistsException] shouldBe thrownBy(relation.create(executor))
        relation.create(executor, true)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        outputPath.resolve("data.csv").toFile.exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        outputPath.resolve("data.csv").toFile.exists() should be (true)

        relation.clean(executor)
        outputPath.resolve("data.csv").toFile.exists() should be (false)
        outputPath.toFile.exists() should be (true)

        relation.destroy(executor)
        outputPath.toFile.exists() should be (false)

        a[FileNotFoundException] shouldBe thrownBy(relation.destroy(executor))
        relation.destroy(executor, true)
    }

    it should "support partitions" in {
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
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))
        outputPath.toFile.exists() should be (false)
        relation.create(executor)
        outputPath.toFile.exists() should be (true)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(executor, df, Map("p_col" -> SingleValue("2")), "overwrite")

        val df_p1 = relation.read(executor, None, Map("p_col" -> SingleValue("1")))
        df_p1.count() should be (0)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))
        val df_p2 = relation.read(executor, None, Map("p_col" -> SingleValue("2")))
        df_p2.count() should be (2)
        df_p1.schema should be (StructType(
            StructField("str_col", StringType, true) ::
                StructField("int_col", IntegerType, true) ::
                StructField("p_col", IntegerType, false) ::
                Nil
        ))

        relation.clean(executor)
        outputPath.resolve("data.csv").toFile.exists() should be (false)
        outputPath.toFile.exists() should be (true)

        relation.destroy(executor)
        outputPath.toFile.exists() should be (false)
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
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("input"))
        val schema = mapping.describe(Map())("main")
        schema should be (ftypes.StructType(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("p_col", ftypes.IntegerType, false)
        )))
    }
}
