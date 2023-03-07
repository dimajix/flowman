/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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
public class RelationIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        return project.map(s -> s + "/" + name).orElse(name);
    }

    public static RelationIdentifier ofString(String relationId) {
        val split = relationId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = relationId.substring(0, split);
            val lastPart = relationId.substring(split + 1);
            return new RelationIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new RelationIdentifier(relationId, Optional.empty());
        }
    }

    public static RelationIdentifier ofProto(com.dimajix.flowman.kernel.proto.RelationIdentifier relationId) {
        if (relationId.hasProject()) {
            return new RelationIdentifier(relationId.getName(), Optional.of(relationId.getProject()));
        }
        else {
            return new RelationIdentifier(relationId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.RelationIdentifier toProto() {
        if (project.isPresent()) {
            return com.dimajix.flowman.kernel.proto.RelationIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.RelationIdentifier.newBuilder()
                .setName(name)
                .build();
        }
    }
}
