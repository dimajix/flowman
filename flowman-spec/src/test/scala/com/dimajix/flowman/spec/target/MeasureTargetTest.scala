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

package com.dimajix.flowman.spec.target

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Measurement
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target


class MeasureTargetTest extends AnyFlatSpec with Matchers with MockFactory {
    "The MeasureTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  custom:
              |    kind: measure
              |    measures:
              |      check_primary_key:
              |        kind: sql
              |        query: "SELECT * FROM somewhere"
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("custom")
        target shouldBe an[MeasureTargetSpec]
    }

    it should "execute measures" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val measure = mock[Measure]
        val target = MeasureTarget(
            Target.Properties(context, "measure"),
            Map("a1" -> measure)
        )

        (measure.requires _).expects().returns(Set())
        (measure.inputs _).expects().atLeastOnce().returns(Seq())
        (measure.context _).expects().returns(context)
        (measure.name _).expects().returns("a1")
        (measure.execute _).expects(*,*).returns(MeasureResult(measure, Seq(Measurement("m1", Map("category" -> "measure", "kind" -> "sql", "name" -> "a1"), 23))))

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        val result = target.execute(execution, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.category should be (Category.TARGET)
        result.status should be (Status.SUCCESS)
        result.exception should be (None)
        result.numFailures should be (0)
        result.numSuccesses should be (1)
        result.numExceptions should be (0)
        result.children.size should be (1)

        val measureResults = result.children.map(_.asInstanceOf[MeasureResult])
        measureResults.size should be (1)
        val measureResult = measureResults.head
        measureResult.measure should be (measure)
        measureResult.name should be ("a1")
        measureResult.category should be (Category.MEASURE)
        measureResult.status should be (Status.SUCCESS)
        measureResult.exception should be (None)
        measureResult.numFailures should be (0)
        measureResult.numSuccesses should be (0)
        measureResult.numExceptions should be (0)
        measureResult.children.size should be (0)
        measureResult.measurements should be (Seq(Measurement("m1", Map("name" -> "a1", "category" -> "measure", "kind" -> "sql", "phase" -> "VERIFY"), 23)))

        val metrics = execution.metrics
        metrics.findMetric(Selector(Some("m1"), Map("name" -> "a1", "category" -> "measure", "kind" -> "sql", "phase" -> "VERIFY"))).size should be (1)
        metrics.findMetric(Selector(Some("m1"), Map("name" -> "a1", "category" -> "measure", "kind" -> "sql" ))).size should be (1)
        metrics.findMetric(Selector(Some("m1"), Map("name" -> "a1", "category" -> "measure"))).size should be (1)
        metrics.findMetric(Selector(Some("m1"), Map("name" -> "a1"))).size should be (1)
        metrics.findMetric(Selector(Some("m1"), Map())).size should be (1)
        val gauges = metrics.findMetric(Selector(Some("m1"), Map("name" -> "a1", "category" -> "measure", "kind" -> "sql", "phase" -> "VERIFY")))
        gauges.head.asInstanceOf[GaugeMetric].value should be (23.0)
    }
}
