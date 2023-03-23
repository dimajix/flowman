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
public class MappingIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        return project.map(s -> s + "/" + name).orElse(name);
    }

    public static MappingIdentifier ofString(String mappingId) {
        val split = mappingId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = mappingId.substring(0, split);
            val lastPart = mappingId.substring(split + 1);
            return new MappingIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new MappingIdentifier(mappingId, Optional.empty());
        }
    }

    public static MappingIdentifier ofProto(com.dimajix.flowman.kernel.proto.MappingIdentifier mappingId) {
        if (mappingId.hasProject()) {
            return new MappingIdentifier(mappingId.getName(), Optional.of(mappingId.getProject()));
        }
        else {
            return new MappingIdentifier(mappingId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.MappingIdentifier toProto() {
        if (project.isPresent()) {
            return com.dimajix.flowman.kernel.proto.MappingIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.MappingIdentifier.newBuilder()
                .setName(name)
                .build();
        }
    }
}
