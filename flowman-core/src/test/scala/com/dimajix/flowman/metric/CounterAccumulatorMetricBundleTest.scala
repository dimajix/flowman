/*
 * Copyright (C) 2019 The Flowman Authors
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.accumulator.CounterAccumulator


class CounterAccumulatorMetricBundleTest extends AnyFlatSpec with Matchers {
    "A CounterAccumulatorMetricBundle" should "provide all metrics" in {
        val accumulator = new CounterAccumulator()
        accumulator.add(Map("a" -> 1l, "b" -> 2l))

        val bundle = new CounterAccumulatorMetricBundle("some_metric", Map("label" -> "value", "sublabel" -> "_"), accumulator, "sublabel")
        bundle.name should be ("some_metric")
        bundle.labels should be (Map("label" -> "value", "sublabel" -> "_"))

        val metrics = bundle.metrics.sortBy(_.labels("sublabel"))
        metrics.size should be (2)
        metrics(0).name should be ("some_metric")
        metrics(0).labels should be (Map("label" -> "value", "sublabel" -> "a"))
        metrics(0) shouldBe a[GaugeMetric]
        metrics(0).asInstanceOf[GaugeMetric].value should be (1.0)
        metrics(1).name should be ("some_metric")
        metrics(1).labels should be (Map("label" -> "value", "sublabel" -> "b"))
        metrics(1) shouldBe a[GaugeMetric]
        metrics(1).asInstanceOf[GaugeMetric].value should be (2.0)
    }
}
