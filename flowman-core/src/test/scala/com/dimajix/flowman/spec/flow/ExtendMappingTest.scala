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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession


class ExtendMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The SqlExtend" should "work" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val xfs = ExtendMapping("myview", Map("new_f" -> "2*_2"))
        xfs.input should be (MappingIdentifier.parse("myview"))
        xfs.columns should be (Map("new_f" -> "2*_2"))
        xfs.dependencies should be (Array(MappingIdentifier.parse("myview")))

        val result = xfs.execute(executor, Map(MappingIdentifier("myview") -> df)).orderBy("_1").collect()
        result.size should be (2)
        result(0) should be (Row("col1", 12, 24))
        result(1) should be (Row("col2", 23, 46))
    }

    it should "correctly interpolate dependencies" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val xfs = ExtendMapping(
            "myview",
            Map(
                "f1" -> "2*_2",
                "f2" -> "2*f1",
                "f3" -> "2*f4 + f2",
                "f4" -> "2*f2"
            )
        )

        val result = xfs.execute(executor, Map(MappingIdentifier("myview") -> df)).orderBy("_1")
        result.schema(0).name should be ("_1")
        result.schema(1).name should be ("_2")
        result.schema(2).name should be ("f1")
        result.schema(3).name should be ("f2")
        result.schema(4).name should be ("f4")
        result.schema(5).name should be ("f3")
        val rows = result.collect()
        rows.size should be (2)
        rows(0) should be (Row("col1", 12, 24, 48, 96, 2*96+48))
        rows(1) should be (Row("col2", 23, 46, 92, 184, 2*184+92))
    }

    it should "replace existing fields" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val xfs = ExtendMapping(
            "myview",
            Map(
                "f1" -> "2*_2",
                "_2" -> "2*_2"
            )
        )

        val result = xfs.execute(executor, Map(MappingIdentifier("myview") -> df)).orderBy("_1")
        result.schema(0).name should be ("_1")
        result.schema(1).name should be ("_2")
        result.schema(2).name should be ("f1")
        val rows = result.collect()
        rows.size should be (2)
        rows(0) should be (Row("col1", 24, 48))
        rows(1) should be (Row("col2", 46, 92))
    }

    it should "detect dependency cycles" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val xfs = ExtendMapping(
            "myview",
            Map(
                "f1" -> "2*_2",
                "f2" -> "2*f1",
                "f3" -> "2*f4 + f2",
                "f4" -> "2*f3"
            )
        )

        a[RuntimeException] should be thrownBy xfs.execute(executor, Map(MappingIdentifier("myview") -> spark.emptyDataFrame))
    }

    "An appropriate Dataflow" should "be readable from YML" in {
        val spec =
            """
              |environment:
              |  - start_ts=2016-06-02T23:40:00
              |mappings:
              |  t0:
              |    kind: provided
              |    table: my_table
              |
              |  t1:
              |    kind: extend
              |    input: t0
              |    columns:
              |      f1: 2*_2
              |      f2: concat(_1, "lala")
              |      f3: unix_timestamp("${start_ts}","yyyy-MM-dd HH:mm:ss")
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)
        implicit val context = executor.context

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)

        executor.spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        )).createOrReplaceTempView("my_table")

        val df2 = executor.instantiate(MappingIdentifier("t1")).orderBy("_1", "_2")
        df2 should not be (null)
    }
}
