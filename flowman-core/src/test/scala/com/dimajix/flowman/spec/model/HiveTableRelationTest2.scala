package com.dimajix.flowman.spec.model

import java.io.File

import org.apache.spark.sql.AnalysisException
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class HiveTableRelationTest2 extends FlatSpec with Matchers with LocalSparkSession {
    "The HiveTableRelation" should "support create, clean and destroy without partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
              |relations:
              |  t0:
              |    kind: hive-table
              |    database: default
              |    table: lala
              |    external: false
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
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test write
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(executor, df)
        spark.read.table("default.lala").count() should be (2)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test 2nd destruction
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))
    }

    it should "support external tables for create, clean and destroy without partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hive-table
               |    database: default
               |    table: lala
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
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test write
        val df = spark.createDataFrame(Seq(("s1", 27), ("s2", 31)))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
        relation.write(executor, df)
        spark.read.table("default.lala").count() should be (2)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test 2nd destruction
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))
    }

    it should "support create, clean and with partitions" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hive-table
               |    database: default
               |    table: lala
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
               |      - name: ipart
               |        type: integer
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val relation = project.relations("t0")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test create
        relation.create(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)
        spark.catalog.getTable("default", "lala") should not be (null)
        spark.read.table("default.lala").count() should be (0)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))

        // Test 2nd destruction
        relation.destroy(executor)
        location.exists() should be (false)
        an[AnalysisException] shouldBe thrownBy(spark.catalog.getTable("default", "lala"))
    }
}
