package com.dimajix.flowman.spec.model

import java.io.File
import java.nio.file.Files
import java.nio.file.Path

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class LocalRelationTest extends FlatSpec with Matchers with BeforeAndAfter with LocalSparkSession {
    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("local_relation_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The LocalRelation" should "be able to create local directories" in {
        val spec =
            s"""
              |relations:
              |  local:
              |    type: local
              |    location: $tempDir/csv/test
              |    filename: data.csv
              |    format: csv
              |    schema:
              |      - name: str_col
              |        type: string
              |      - name: int_col
              |        type: integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context
        val relation = project.relations("local")

        relation.create(executor)
        new File(tempDir.toFile, "csv/test").exists() should be (true)

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        new File(tempDir.toFile, "csv/test/data.csv").exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        new File(tempDir.toFile, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir.toFile, "csv/test").exists() should be (false)
    }

    it should "also support file:/// schema" in {
        val spec =
            s"""
               |relations:
               |  local:
               |    type: local
               |    location: file:///$tempDir/csv/test
               |    filename: data.csv
               |    format: csv
               |    schema:
               |      - name: str_col
               |        type: string
               |      - name: int_col
               |        type: integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context
        val relation = project.relations("local")

        relation.create(executor)
        new File(tempDir.toFile, "csv/test").exists() should be (true)

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        new File(tempDir.toFile, "csv/test/data.csv").exists() should be (false)
        relation.write(executor, df, Map(), "overwrite")
        new File(tempDir.toFile, "csv/test/data.csv").exists() should be (true)

        relation.destroy(executor)
        new File(tempDir.toFile, "csv/test").exists() should be (false)
    }
}
