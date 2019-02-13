package com.dimajix.flowman.spec.model

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class HiveTableRelationTest2 extends FlatSpec with Matchers with LocalSparkSession {
    "The HiveTableRelation" should "support create" in {
        val location = new File(tempDir, "hive/default/lala")
        val spec =
            s"""
              |relations:
              |  t0:
              |    kind: hive-table
              |    database: default
              |    table: lala
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

        // Test create
        relation.create(executor)
        location.exists() should be (true)

        // Test clean
        relation.clean(executor)
        location.exists() should be (true)

        // Test destroy
        relation.destroy(executor)
        location.exists() should be (false)

        // Test 2nd destruction
        relation.destroy(executor)
        location.exists() should be (false)
    }
}
