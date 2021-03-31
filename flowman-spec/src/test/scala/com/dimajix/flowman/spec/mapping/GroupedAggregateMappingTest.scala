/*
 * Copyright 2021 Kaya Kupferschmidt
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class GroupedAggregateMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The GroupedAggregateMapping" should "be parsable" in {
        val spec =
            """
              |kind: groupedAggregate
              |input: facts_delivery
              |aggregations:
              |  imps: "SUM(imps)"
              |  clicks: "SUM(clicks)"
              |
              |groups:
              |  - name: adpod
              |    dimensions:
              |      - device_setting
              |      - network
              |    aggregations:
              |      - imps
              |
              |  - name: delivery
              |    dimensions:
              |      - device_setting
              |      - network
              |    aggregations:
              |      - imps
              |      - clicks
              |""".stripMargin

        val mappingSpec = ObjectMapper.parse[MappingSpec](spec)
        mappingSpec shouldBe a[GroupedAggregateMappingSpec]
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val mapping = GroupedAggregateMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("data"),
            Seq(
                GroupedAggregateMapping.Group(
                    name = "g1",
                    dimensions = Seq("_1", "_2"),
                    aggregations = Seq("count")
                ),
                GroupedAggregateMapping.Group(
                    name = "g2",
                    dimensions = Seq("_1"),
                    aggregations = Seq("count")
                ),
                GroupedAggregateMapping.Group(
                    name = "g3",
                    dimensions = Seq("_1", "_3"),
                    aggregations = Seq()
                )
            ),
            Map(
                "count" -> "count(1)",
                "sum" -> "sum(1)"
            )
        )

        mapping.input should be (MappingOutputIdentifier("data"))
        mapping.outputs.toSet should be (Set("g1", "g2", "g3", "cache"))

        val data = execution.spark.createDataFrame(Seq(
            ("c1_v1", "c2_v1", "c3_v1", 23.0),
            ("c1_v1", "c2_v1", "c3_v2", 18.0),
            ("c1_v2", "c2_v1", "c3_v3", 123.0),
            ("c1_v2", "c2_v2", "c3_v4", 118.0)
        ))

        val result = mapping.execute(execution, Map(MappingOutputIdentifier("data") -> data))
        result.keySet should be (Set("g1", "g2", "g3", "cache"))

        result("g1").schema should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", StringType),
            StructField("count", LongType, false)
        )))
        result("g1").collect().toSet should be (Set(
            Row("c1_v1", "c2_v1", 2l),
            Row("c1_v2", "c2_v2", 1l),
            Row("c1_v2", "c2_v1", 1l)
        ))

        result("g2").schema should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("count", LongType, false)
        )))
        result("g2").collect().toSet should be (Set(
            Row("c1_v1", 2l),
            Row("c1_v2", 2l)
        ))

        result("g3").schema should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_3", StringType),
            StructField("count", LongType, false),
            StructField("sum", LongType, true)
        )))
        result("g3").collect().toSet should be (Set(
            Row("c1_v1", "c3_v1", 1l, 1l),
            Row("c1_v1", "c3_v2", 1l, 1l),
            Row("c1_v2", "c3_v3", 1l, 1l),
            Row("c1_v2", "c3_v4", 1l, 1l)
        ))
        result("cache").count() should be (9)
    }
}
