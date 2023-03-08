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
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Value;


@Value
public class Mapping {
    Optional<String> project;
    String name;
    String kind;
    List<String> outputs;
    List<MappingOutputIdentifier> inputs;
    String cache;
    boolean broadcast;
    boolean checkpoint;
    List<ResourceIdentifier> required;

    public static Mapping ofProto(com.dimajix.flowman.kernel.proto.mapping.MappingDetails mapping) {
        return new Mapping(
            mapping.hasProject() ? Optional.of(mapping.getProject()) : Optional.empty(),
            mapping.getName(),
            mapping.getKind(),
            mapping.getOutputsList(),
            mapping.getInputsList().stream().map(MappingOutputIdentifier::ofProto).collect(Collectors.toList()),
            mapping.getCache(),
            mapping.getBroadcast(),
            mapping.getCheckpoint(),
            mapping.getRequiredList().stream().map(ResourceIdentifier::ofProto).collect(Collectors.toList())
        );
    }
}
