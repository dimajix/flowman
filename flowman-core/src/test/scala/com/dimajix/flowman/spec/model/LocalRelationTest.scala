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

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.testing.LocalSparkSession


class LocalRelationTest extends FlatSpec with Matchers with BeforeAndAfter with LocalSparkSession {
    "The LocalRelation" should "be able to create local directories" in {
        val outputPath = Paths.get(tempDir.toString, "csv", "test")
        val spec =
            s"""
              |relations:
              |  local:
              |    kind: local
              |    location: $outputPath
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
        val executor = session.getExecutor(project)
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))

        val localRelation = relation.asInstanceOf[LocalRelation]
        localRelation.location should be (new Path(outputPath.toUri))
        localRelation.pattern should be ("data.csv")

        outputPath.toFile.exists() should be (false)
        relation.create(executor)
        outputPath.toFile.exists() should be (true)
        outputPath.resolve("data.csv").toFile.exists() should be (false)

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
    }

    it should "work without a pattern" in {
        val spec =
            s"""
               |relations:
               |  local:
               |    kind: local
               |    location: $tempDir/csv/test/data.csv
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
        val executor = session.getExecutor(project)
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))

        val localRelation = relation.asInstanceOf[LocalRelation]
        localRelation.location should be (new Path(tempDir.toURI.toString + "/csv/test/data.csv"))
        localRelation.pattern should be (null)

        relation.create(executor)
        new File(tempDir, "csv/test").exists() should be (true)
        new File(tempDir, "csv/test/data.csv").exists() should be (false)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        new File(tempDir, "csv/test/data.csv").exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        new File(tempDir, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir, "csv/test").exists() should be (false)
    }

    it should "also support URI schema with (empty) authority" in {
        val location = new Path("file", "", tempDir.getPath).toUri
        location.toString should startWith ("file:///")

        val spec =
            s"""
               |relations:
               |  local:
               |    kind: local
               |    location: $location/csv/test
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
        val executor = session.getExecutor(project)
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))

        val localRelation = relation.asInstanceOf[LocalRelation]
        localRelation.location should be (new Path(location.toString + "/csv/test"))
        localRelation.pattern should be ("data.csv")

        relation.create(executor)
        new File(tempDir, "csv/test").exists() should be (true)
        new File(tempDir, "csv/test/data.csv").exists() should be (false)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        new File(tempDir, "csv/test/data.csv").exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        new File(tempDir, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir, "csv/test").exists() should be (false)
    }

    it should "also support URI schema without authority" in {
        val location = new Path("file", null, tempDir.getPath).toUri
        location.toString should not startWith ("file:///")
        location.toString should startWith ("file:/")

        val spec =
            s"""
               |relations:
               |  local:
               |    kind: local
               |    location: $location/csv/test
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
        val executor = session.getExecutor(project)
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))

        val localRelation = relation.asInstanceOf[LocalRelation]
        localRelation.location should be (new Path(location.toString + "/csv/test"))
        localRelation.pattern should be ("data.csv")

        relation.create(executor)
        new File(tempDir, "csv/test").exists() should be (true)
        new File(tempDir, "csv/test/data.csv").exists() should be (false)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        new File(tempDir, "csv/test/data.csv").exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        new File(tempDir, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir, "csv/test").exists() should be (false)
    }
}
