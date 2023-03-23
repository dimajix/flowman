/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Value;


@Value
public class Job {
    @Value
    public static class Parameter {
        public static Parameter ofProto(com.dimajix.flowman.kernel.proto.job.JobParameter param) {
            return new Parameter(
                param.getName(),
                param.getType(),
                param.hasGranularity() ? Optional.of(param.getGranularity()) : Optional.empty(),
                param.hasDefault() ? Optional.of(param.getDefault()) : Optional.empty(),
                param.hasDescription() ? Optional.of(param.getDescription()) : Optional.empty()
            );
        }

        String name;
        String type;
        Optional<String> granularity;
        Optional<String> defaultValue;
        Optional<String> description;
    };

    public static Job ofProto(com.dimajix.flowman.kernel.proto.job.JobDetails job) {
        return new Job(
            job.getProject(),
            job.getName(),
            job.hasDescription() ? Optional.of(job.getDescription()) : Optional.empty(),
            job.getTargetsList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList()),
            job.getParametersList().stream().map(Parameter::ofProto).collect(Collectors.toList()),
            job.getEnvironmentMap()
        );
    }

    String project;
    String name;
    Optional<String> description;
    List<TargetIdentifier> targets;
    List<Parameter> parameters;
    Map<String,String> environment;
}
