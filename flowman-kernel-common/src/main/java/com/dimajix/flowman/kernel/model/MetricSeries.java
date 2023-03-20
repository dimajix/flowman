/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.kernel.model;

import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Value
public class MetricSeries {
    String metric;
    String namespace;
    String project;
    String job;
    Phase phase;
    Map<String,String> labels;
    List<Measurement> measurements;

    public static MetricSeries ofProto(com.dimajix.flowman.kernel.proto.MetricSeries series) {
        return new MetricSeries(
            series.getMetric(),
            series.getNamespace(),
            series.getProject(),
            series.getJob(),
            Phase.ofProto(series.getPhase()),
            series.getLabelsMap(),
            series.getMeasurementsList().stream().map(Measurement::ofProto).collect(Collectors.toList())
        );
    }
}
