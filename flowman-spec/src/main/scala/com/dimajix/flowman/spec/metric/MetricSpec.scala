/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.metric

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSelection
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.spec.Spec


class MetricSpec extends Spec[MetricSelection] {
    @JsonProperty(value = "name", required = true) var name:Option[String] = None
    @JsonProperty(value = "labels", required = false) var labels:Map[String,String] = Map()
    @JsonProperty(value = "selector", required = true) var selector:SelectorSpec = _

    def instantiate(context:Context) : MetricSelection = {
        MetricSelection(
            context.evaluate(name),
            selector.instantiate(context),
            labels
        )
    }
}


class SelectorSpec extends Spec[Selector] {
    @JsonProperty(value = "name", required = false) var name: Option[String] = None
    @JsonProperty(value = "labels", required = false) var labels: Map[String, String] = Map()

    def instantiate(context: Context): Selector = {
        Selector(
            name.map(context.evaluate).map(_.r),
            context.evaluate(labels).map { case(k,v) => k -> v.r }
        )
    }
}


class MetricBoardSpec extends Spec[MetricBoard] {
    @JsonProperty(value = "labels", required = false) private var labels: Map[String, String] = Map()
    @JsonProperty(value = "metrics", required = false) private var metrics: Seq[MetricSpec] = Seq()

    def instantiate(context: Context): MetricBoard = {
        MetricBoard(
            context,
            labels,
            metrics.map(_.instantiate(context))
        )
    }
}
