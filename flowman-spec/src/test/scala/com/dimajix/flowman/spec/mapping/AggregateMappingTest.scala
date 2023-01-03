/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import scala.collection.immutable.ListMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.{types => ft}
import com.dimajix.spark.testing.LocalSparkSession


class AggregateMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The Aggregation" should "group and aggregate data" in {
        val df = spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", 12, 23.0),
            ("c1_v1", "c2_v1", 13, 18.0),
            ("c1_v2", "c2_v1", 118, 123.0),
            ("c1_v2", "c2_v2", 113, 118.0)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val xfs = AggregateMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq("_1", "_2"),
            ListMap(
                "agg3" -> "sum(_3)",
                "agg4" -> "sum(_4)",
                "agg5" -> "sum(_4)",
                "agg6" -> "sum(_4)",
                "agg7" -> "sum(_4)"
            )
        )

        xfs.input should be (MappingOutputIdentifier("myview"))
        xfs.outputs should be (Set("main"))
        xfs.dimensions should be (Array("_1", "_2"))
        xfs.aggregations should be (Map("agg3" -> "sum(_3)", "agg4" -> "sum(_4)", "agg5" -> "sum(_4)", "agg6" -> "sum(_4)", "agg7" -> "sum(_4)"))
        xfs.inputs should be (Set(MappingOutputIdentifier("myview")))

        val df2 = xfs.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("_1", "_2")
        df2.schema should be (
            StructType(Seq(
                StructField("_1", StringType),
                StructField("_2", StringType),
                StructField("agg3", LongType),
                StructField("agg4", DoubleType),
                StructField("agg5", DoubleType),
                StructField("agg6", DoubleType),
                StructField("agg7", DoubleType)
            ))
        )
        val result = df2.collect()
        result.size should be (3)
        result(0) should be (Row("c1_v1", "c2_v1", 25l, 41.0, 41.0, 41.0, 41.0))
        result(1) should be (Row("c1_v2", "c2_v1", 118l, 123.0, 123.0, 123.0, 123.0))
        result(2) should be (Row("c1_v2", "c2_v2", 113l, 118.0, 118.0, 118.0, 118.0))

        session.shutdown()
    }

    it should "support expressions in dimensions" in {
        val df = spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", 12, 23.0),
            ("c1_v1", "c2_v1", 13, 18.0),
            ("c1_v2", "c2_v1", 118, 123.0),
            ("c1_v2", "c2_v2", 113, 118.0)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val xfs = AggregateMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq("_1 AS dim1", "upper(_2) AS dim2"),
            ListMap(
                "agg3" -> "sum(_3)"
            )
        )

        xfs.input should be (MappingOutputIdentifier("myview"))
        xfs.outputs should be (Set("main"))
        xfs.dimensions should be (Seq("_1 AS dim1", "upper(_2) AS dim2"))
        xfs.aggregations should be (Map("agg3" -> "sum(_3)"))
        xfs.inputs should be (Set(MappingOutputIdentifier("myview")))

        val df2 = xfs.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("dim1", "dim2")
        df2.schema should be (
            StructType(Seq(
                StructField("dim1", StringType),
                StructField("dim2", StringType),
                StructField("agg3", LongType)
            ))
        )
        val result = df2.collect()
        result.size should be (3)
        result(0) should be (Row("c1_v1", "C2_V1", 25l))
        result(1) should be (Row("c1_v2", "C2_V1", 118l))
        result(2) should be (Row("c1_v2", "C2_V2", 113l))

        session.shutdown()
    }

    it should "provide an appropriate description" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val xfs = AggregateMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input"),
            Seq("country", "year"),
            ListMap(
                "total_net_amount" -> "sum(net_amount)",
                "total_gross_amount" -> "sum(gross_amount)",
                "avg_gross_amount" -> "avg(gross_amount)",
                "min_gross_amount" -> "min(gross_amount)"
            )
        )

        val input = ft.StructType(Seq(
            ft.Field("country", ft.VarcharType(3), description=Some("The country")),
            ft.Field("year", ft.IntegerType, nullable=false, description=Some("The year")),
            ft.Field("net_amount", ft.IntegerType, description=Some("The net amount")),
            ft.Field("gross_amount", ft.IntegerType, description=Some("The gross amount"))
        ))
        val result = xfs.describe(executor, Map(MappingOutputIdentifier("input") -> input))

        result should be (Map("main" -> ft.StructType(Seq(
            ft.Field("country", ft.VarcharType(3), description=Some("The country")),
            ft.Field("year", ft.IntegerType, nullable=false, description=Some("The year")),
            ft.Field("total_net_amount", ft.LongType, description=Some("The net amount (SUM)")),
            ft.Field("total_gross_amount", ft.LongType, description=Some("The gross amount (SUM)")),
            ft.Field("avg_gross_amount", ft.DoubleType, description=Some("The gross amount (AVG)")),
            ft.Field("min_gross_amount", ft.IntegerType, description=Some("The gross amount (MIN)"))
        ))))

        session.shutdown()
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
        val executor = session.execution
        val context = session.getContext(project)

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)

        executor.spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", 12, 23.0),
            ("c1_v1", "c2_v1", 13, 18.0),
            ("c1_v2", "c2_v1", 118, 123.0),
            ("c1_v2", "c2_v2", 113, 118.0)
        )).createTempView("my_table")

        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping should not be null
        mapping.context should be theSameInstanceAs (context)

        val df2 = executor.instantiate(mapping, "main")
            .orderBy("_1", "_2")
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

        session.shutdown()
    }
}
