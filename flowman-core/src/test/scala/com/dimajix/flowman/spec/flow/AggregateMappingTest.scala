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
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession


class AggregateMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The Aggregation" should "group and aggregate data" in {
        val df = spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", 12, 23.0),
            ("c1_v1", "c2_v1", 13, 18.0),
            ("c1_v2", "c2_v1", 118, 123.0),
            ("c1_v2", "c2_v2", 113, 118.0)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val xfs = AggregateMapping(
            "myview",
            Seq("_1", "_2"),
            Map("agg3" -> "sum(_3)", "agg4" -> "sum(_4)")
        )

        xfs.input should be (MappingIdentifier("myview"))
        xfs.dimensions should be (Array("_1", "_2"))
        xfs.aggregations should be (Map("agg3" -> "sum(_3)", "agg4" -> "sum(_4)"))
        xfs.dependencies should be (Array(MappingIdentifier("myview")))

        val df2 = xfs.execute(executor, Map(MappingIdentifier("myview") -> df)).orderBy("_1", "_2")
        df2.schema should be (
            StructType(
                StructField("_1", StringType) ::
                StructField("_2", StringType) ::
                StructField("agg3", LongType) ::
                StructField("agg4", DoubleType) :: Nil
            )
        )
        val result = df2.collect()
        result.size should be (3)
        result(0) should be (Row("c1_v1", "c2_v1", 25l, 41.0))
        result(1) should be (Row("c1_v2", "c2_v1", 118l, 123.0))
        result(2) should be (Row("c1_v2", "c2_v2", 113l, 118.0))
    }

    "An appropriate project" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: provided
              |    table: my_table
              |
              |  t1:
              |    kind: aggregate
              |    input: t0
              |    dimensions:
              |      - _1
              |      - _2
              |    aggregations:
              |      f1: sum(_3)
              |      f2: sum(_4)
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)

        executor.spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", 12, 23.0),
            ("c1_v1", "c2_v1", 13, 18.0),
            ("c1_v2", "c2_v1", 118, 123.0),
            ("c1_v2", "c2_v2", 113, 118.0)
        )).createTempView("my_table")

        val df2 = executor.instantiate(MappingIdentifier("t1")).orderBy("_1", "_2")
        df2.schema should be (
            StructType(
                StructField("_1", StringType) ::
                    StructField("_2", StringType) ::
                    StructField("f1", LongType) ::
                    StructField("f2", DoubleType) :: Nil
            )
        )
        val result = df2.collect()
        result.size should be (3)
        result(0) should be (Row("c1_v1", "c2_v1", 25l, 41.0))
        result(1) should be (Row("c1_v2", "c2_v1", 118l, 123.0))
        result(2) should be (Row("c1_v2", "c2_v2", 113l, 118.0))
    }
}
