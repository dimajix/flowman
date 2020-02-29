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
import java.io.PrintWriter
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.testing.LocalSparkSession


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
        val executor = session.executor
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
        relation.write(executor, df, Map(), OutputMode.OVERWRITE)
        outputPath.resolve("data.csv").toFile.exists() should be (true)

        relation.truncate(executor)
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
        val executor = session.executor
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
        relation.write(executor, df, Map(), OutputMode.OVERWRITE)
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
        val executor = session.executor
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
        relation.write(executor, df, Map(), OutputMode.OVERWRITE)
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
        val executor = session.executor
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
        relation.write(executor, df, Map(), OutputMode.OVERWRITE)
        new File(tempDir, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir, "csv/test").exists() should be (false)
    }

    it should "support using wildcards for unspecified partitions" in {
        val spark = this.spark
        import spark.implicits._

        val outputPath = Paths.get(tempDir.toString, "partitions_test")
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

        val spec =
            s"""
               |relations:
               |  local:
               |    kind: local
               |    location: ${outputPath.toUri}
               |    pattern: p1=$$p1/p2=$$p2/*
               |    format: csv
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: value
               |          type: string
               |    partitions:
               |        - name: p1
               |          type: integer
               |        - name: p2
               |          type: integer
               |""".stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("local"))
        relation.resources(Map("p1" -> SingleValue("1"), "p2" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofLocal(new Path(outputPath.toUri.toString, "p1=1/p2=1/*"))
        ))
        relation.resources(Map("p1" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofLocal(new Path(outputPath.toUri.toString, "p1=1/p2=*/*"))
        ))
        relation.resources(Map("p2" -> SingleValue("1"))) should be (Set(
            ResourceIdentifier.ofLocal(new Path(outputPath.toUri.toString, "p1=*/p2=1/*"))
        ))
        relation.resources(Map()) should be (Set(
            ResourceIdentifier.ofLocal(new Path(outputPath.toUri.toString, "p1=*/p2=*/*"))
        ))

        relation.create(executor, true)
        relation.migrate(executor)

        val df1 = relation.read(executor, None, Map("p1" -> SingleValue("1"), "p2" -> SingleValue("1")))
        df1.as[(String,Int,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1,1),
            ("p1=1/p2=1/112.txt",1,1)
        ))

        val df2 = relation.read(executor, None, Map("p1" -> SingleValue("1")))
        df2.as[(String,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1),
            ("p1=1/p2=1/112.txt",1),
            ("p1=1/p2=2/121.txt",1),
            ("p1=1/p2=2/122.txt",1)
        ))

        val df3 = relation.read(executor, None, Map("p2" -> SingleValue("1")))
        df3.as[(String,Int)].collect().sorted should be (Seq(
            ("p1=1/p2=1/111.txt",1),
            ("p1=1/p2=1/112.txt",1),
            ("p1=2/p2=1/211.txt",1),
            ("p1=2/p2=1/212.txt",1)
        ))

        val df4 = relation.read(executor, None, Map())
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

        relation.truncate(executor, Map("p2" -> SingleValue("1")))
        val df5 = relation.read(executor, None, Map())
        df5.as[String].collect().sorted should be (Seq(
            ("p1=1/p2=2/121.txt"),
            ("p1=1/p2=2/122.txt"),
            ("p1=2/p2=2/221.txt"),
            ("p1=2/p2=2/222.txt")
        ))

        relation.destroy(executor)
    }
}
