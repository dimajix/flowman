/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.spark.accumulator.CounterAccumulator


class MetricBoardTest extends FlatSpec with Matchers {
    "A MetricBoard" should "return relabelled metrics" in  {
        val session = Session.builder()
            .withEnvironment("env_var", "env_value")
            .build()

        val registry = session.metrics
        val context = session.context

        val accumulator1 = new CounterAccumulator()
        accumulator1.add(Map("a" -> 1l, "b" -> 2l))
        registry.addBundle(CounterAccumulatorMetricBundle("some_metric", Map("raw_label" -> "raw_value"), accumulator1, "sublabel"))
        val selections = Seq(
            MetricSelection(
                "m1",
                Selector(Some("some_metric"),
                    Map("raw_label" -> "raw_value", "sublabel" -> "a")
                ),
                Map("rl" -> "$raw_label", "sl" -> "$sublabel", "ev" -> "$env_var")
            )
        )
        val board = MetricBoard(context, Map("board_label" -> "board1", "status" -> "$status"), selections)

        board.metrics(registry, Status.RUNNING) should be (
            Seq(FixedGaugeMetric("m1", Map("board_label" -> "board1", "rl" -> "raw_value", "sl" -> "a", "ev" -> "env_value", "status" -> "running"), 1l))
        )
    }
}
