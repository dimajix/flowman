/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.metric

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.accumulator.CounterAccumulator


class MetricSystemTest extends FlatSpec with Matchers {
    "The MetricSystem" should "return all metrics" in {
        val registry = new MetricSystem

        val accumulator1 = new CounterAccumulator()
        accumulator1.add(Map("a" -> 1l, "b" -> 2l))
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric", Map("label" -> "acc1"), accumulator1, "sublabel"))

        val accumulator2 = new CounterAccumulator()
        accumulator2.add(Map("a" -> 3l, "c" -> 2l))
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric", Map("label" -> "acc2"), accumulator2, "sublabel"))

        val metrics = registry.metrics.sortBy(m => m.labels("label") + ";" + m.labels("sublabel"))
        metrics.size should be (4)
        metrics(0).name should be ("some_metric")
        metrics(0).labels should be (Map("label" -> "acc1", "sublabel" -> "a"))
        metrics(1).name should be ("some_metric")
        metrics(1).labels should be (Map("label" -> "acc1", "sublabel" -> "b"))
        metrics(2).name should be ("some_metric")
        metrics(2).labels should be (Map("label" -> "acc2", "sublabel" -> "a"))
        metrics(3).name should be ("some_metric")
        metrics(3).labels should be (Map("label" -> "acc2", "sublabel" -> "c"))
    }

    it should "support queries" in {
        val registry = new MetricSystem

        val accumulator1 = new CounterAccumulator()
        accumulator1.add(Map("a" -> 1l, "b" -> 2l))
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric_1", Map("label" -> "acc1"), accumulator1, "sublabel"))

        val accumulator2 = new CounterAccumulator()
        accumulator2.add(Map("a" -> 3l, "c" -> 2l))
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric_2", Map("label" -> "acc2"), accumulator2, "sublabel"))

        val r1 = registry.findMetric(Selector())
        r1.size should be (4)

        val r2 = registry.findMetric(Selector(labels=Map("no_such_label" -> "xyz")))
        r2.size should be (0)

        val r3 = registry.findMetric(Selector(labels=Map("label" -> "no_such_value")))
        r3.size should be (0)

        val r4 = registry.findMetric(Selector(labels=Map("sublabel" -> "no_such_value")))
        r4.size should be (0)

        val r5 = registry.findMetric(Selector(labels=Map("label" -> "acc1")))
        r5.size should be (2)
        r5.forall(_.labels("label") == "acc1") should be (true)

        val r6 = registry.findMetric(Selector(labels=Map("sublabel" -> "a")))
        r6.size should be (2)
        r6.forall(_.labels("sublabel") == "a") should be (true)

        val r7 = registry.findMetric(Selector(labels=Map("label" -> "acc1", "sublabel" -> "a")))
        r7.size should be (1)
        r7.forall(m => m.labels("label") == "acc1" && m.labels("sublabel") == "a") should be (true)

        val r8 = registry.findMetric(Selector(name=Some("no_such_metric")))
        r8.size should be (0)

        val r9 = registry.findMetric(Selector(name=Some("some_metric_1")))
        r9.size should be (2)

        val r10 = registry.findMetric(Selector(name=Some("some_metric_1"), labels=Map("label" -> "acc1")))
        r10.size should be (2)

        val r11 = registry.findMetric(Selector(name=Some("some_metric_1"), labels=Map("label" -> "acc2")))
        r11.size should be (0)
    }

    it should "support finding bundles" in {
        val registry = new MetricSystem

        val accumulator1 = new CounterAccumulator()
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric_1", Map("label" -> "acc1"), accumulator1, "sublabel"))

        val accumulator2 = new CounterAccumulator()
        registry.addBundle(new CounterAccumulatorMetricBundle("some_metric_2", Map("label" -> "acc2"), accumulator2, "sublabel"))

        registry.findBundle(Selector()).size should be (2)
        registry.findBundle(Selector(labels=Map("label" -> "acc2"))).size should be (1)
        registry.findBundle(Selector(labels=Map("label" -> "acc3"))).size should be (0)
        registry.findBundle(Selector(name=Some("no_such_metric"))).size should be (0)
        registry.findBundle(Selector(name=Some("some_metric_1"))).size should be (1)
        registry.findBundle(Selector(name=Some("some_metric_1"), labels=Map("label" -> "acc1"))).size should be (1)
        registry.findBundle(Selector(name=Some("some_metric_1"), labels=Map("label" -> "acc2"))).size should be (0)
    }
}
