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
public class Test {
    Optional<String> project;
    String name;
    Optional<String> description;
    Map<String,String> environment;
    List<MappingIdentifier> overrideMappings;
    List<RelationIdentifier> overrideRelations;
    List<TargetIdentifier> fixtureTargets;
    List<TargetIdentifier> buildTargets;
    List<String> assertions;

    public static Test ofProto(com.dimajix.flowman.kernel.proto.test.TestDetails test) {
        return new Test(
            test.hasProject() ? Optional.of(test.getProject()) : Optional.empty(),
            test.getName(),
            test.hasDescription() ? Optional.of(test.getDescription()) : Optional.empty(),
            test.getEnvironmentMap(),
            test.getOverrideMappingsList().stream().map(MappingIdentifier::ofProto).collect(Collectors.toList()),
            test.getOverrideRelationsList().stream().map(RelationIdentifier::ofProto).collect(Collectors.toList()),
            test.getFixtureTargetsList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList()),
            test.getBuildTargetsList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList()),
            test.getAssertionsList()
        );
    }
}
