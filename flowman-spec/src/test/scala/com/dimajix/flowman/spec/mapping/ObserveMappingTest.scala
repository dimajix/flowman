/*
 * Copyright 2022 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ObserveMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ObserveMapping" should "be parsable" in {
        val yaml =
            """
              |kind: observe
              |input: some_mapping
              |measures:
              |  min: min(lala)
              |  count: "count(*)"
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val context = session.context
        val spec = ObjectMapper.parse[MappingSpec](yaml)

        spec shouldBe a[ObserveMappingSpec]

        val mapping = spec.instantiate(context, None).asInstanceOf[ObserveMapping]
        mapping.inputs should be (Set(MappingOutputIdentifier("some_mapping")))
        mapping.kind should be ("observe")
        mapping.measures should be (Map("min" -> "min(lala)", "count" -> "count(*)"))

        session.shutdown()
    }

    it should "publish metrics" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val mapping = ObserveMapping(
            Mapping.Properties(context, "observe", "mapping"),
            MappingOutputIdentifier("some_input"),
            Map("min" -> "min(_2)", "count" -> "count(*)")
        )
        val result = mapping.execute(execution, Map(MappingOutputIdentifier("some_input") -> df))("main")
        result.count()

        // Spark listeners are fired asynchronously, so give them some time
        Thread.sleep(1000)

        val minGauge = session.metrics.findMetric(Selector("min", Map.empty[String,String])).head.asInstanceOf[GaugeMetric]

        if (spark.version >= "3.0") {
            minGauge.value should be (12.0)
        }
        minGauge.name should be ("min")
        minGauge.labels should be (Map("name" -> "observe", "category" -> "mapping", "kind" -> "mapping"))

        val countGauge = session.metrics.findMetric(Selector("count", Map.empty[String,String])).head.asInstanceOf[GaugeMetric]
        if (spark.version >= "3.0") {
            countGauge.value should be(2.0)
        }
        countGauge.name should be("count")
        countGauge.labels should be(Map("name" -> "observe", "category" -> "mapping", "kind" -> "mapping"))

        session.shutdown()
    }

    it should "describe its output" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val mapping = ObserveMapping(
            Mapping.Properties(context, "observe", "mapping"),
            MappingOutputIdentifier("some_input"),
            Map("min" -> "min(_2)", "count" -> "count(*)")
        )

        val expectedSchema = StructType(Seq(
            Field("second", LongType, false),
            Field("first", StringType, true)
        ))

        val result = mapping.describe(execution, Map(MappingOutputIdentifier("some_input") -> expectedSchema))
        result should be (Map("main" -> expectedSchema))

        session.shutdown()
    }
}
