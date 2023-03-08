/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.spec.measure

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Measurement
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class SqlMeasureTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The SqlMeasure" should "be parseable" in {
        val spec =
            """
              |kind: sql
              |query: SELECT * FROM lala
              |""".stripMargin

        val measureSpec = ObjectMapper.parse[MeasureSpec](spec)
        measureSpec shouldBe a[SqlMeasureSpec]

        val context = RootContext.builder().build()
        val measure = measureSpec.instantiate(context).asInstanceOf[SqlMeasure]
        measure.name should be ("")
        measure.query should be ("SELECT * FROM lala")
        measure.inputs should be (Seq(MappingOutputIdentifier("lala")))
        measure.requires should be (Set())
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val measure = SqlMeasure(
            Measure.Properties(context, "m1", "sql"),
            query =
                """
                  |SELECT
                  | "value" AS label,
                  | COUNT(*) AS count,
                  | SUM(id) AS sum
                  |FROM mx""".stripMargin
        )

        measure.inputs should be (Seq(MappingOutputIdentifier("mx")))
        measure.requires should be (Set())

        val mx = execution.spark.range(2).toDF()
        val result = measure.execute(execution, Map(MappingOutputIdentifier("mx") -> mx))
        result.withoutTime should be (
            MeasureResult(
                measure,
                Seq(
                    Measurement("count", Map("category" -> "measure", "kind" -> "sql", "name" -> "m1", "label" -> "value"), 2),
                    Measurement("sum", Map("category" -> "measure", "kind" -> "sql", "name" -> "m1", "label" -> "value"), 1)
                )
            ).withoutTime
        )

        session.shutdown()
    }
}
