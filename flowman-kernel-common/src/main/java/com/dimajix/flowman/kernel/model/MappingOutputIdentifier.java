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

import java.util.Optional;

import lombok.Value;
import lombok.val;


@Value
public class MappingOutputIdentifier {
    String name;
    Optional<String> project;
    String output;


    public MappingIdentifier getMapping() {
        return new MappingIdentifier(name, project);
    }

    public static MappingOutputIdentifier ofString(String mappingId) {
        val projectTailParts = mappingId.split("/");
        val mappingOutput = projectTailParts[projectTailParts.length - 1];
        val mappingOutputParts = mappingOutput.split(":");

        Optional<String> project = projectTailParts.length > 1 ? Optional.of(projectTailParts[0]) : Optional.empty();
        val mapping = mappingOutputParts[0];
        val output = mappingOutputParts.length > 1 ? mappingOutputParts[1] : "main";
        return new MappingOutputIdentifier(mapping, project, output);
    }

    public static MappingOutputIdentifier ofProto(com.dimajix.flowman.kernel.proto.MappingOutputIdentifier id) {
        return new MappingOutputIdentifier(
            id.getName(),
            id.hasProject() ? Optional.of(id.getProject()) : Optional.empty(),
            id.getOutput()
        );
    }
}
