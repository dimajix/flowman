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
public class TargetIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        return project.map(s -> s + "/" + name).orElse(name);
    }

    public static TargetIdentifier ofString(String targetId) {
        val split = targetId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = targetId.substring(0, split);
            val lastPart = targetId.substring(split + 1);
            return new TargetIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new TargetIdentifier(targetId, Optional.empty());
        }
    }

    public static TargetIdentifier ofProto(com.dimajix.flowman.kernel.proto.TargetIdentifier targetId) {
        if (targetId.hasProject()) {
            return new TargetIdentifier(targetId.getName(), Optional.of(targetId.getProject()));
        }
        else {
            return new TargetIdentifier(targetId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.TargetIdentifier toProto() {
        if (project.isPresent()) {
            return com.dimajix.flowman.kernel.proto.TargetIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.TargetIdentifier.newBuilder()
                .setName(name)
                .build();
        }
    }
}
