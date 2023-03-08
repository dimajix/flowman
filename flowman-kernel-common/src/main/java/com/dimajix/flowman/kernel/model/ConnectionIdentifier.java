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
public class ConnectionIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        return project.map(s -> s + "/" + name).orElse(name);
    }

    public static ConnectionIdentifier ofString(String connectionId) {
        val split = connectionId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = connectionId.substring(0, split);
            val lastPart = connectionId.substring(split + 1);
            return new ConnectionIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new ConnectionIdentifier(connectionId, Optional.empty());
        }
    }

    public static ConnectionIdentifier ofProto(com.dimajix.flowman.kernel.proto.ConnectionIdentifier connectionId) {
        if (connectionId.hasProject()) {
            return new ConnectionIdentifier(connectionId.getName(), Optional.of(connectionId.getProject()));
        }
        else {
            return new ConnectionIdentifier(connectionId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.ConnectionIdentifier toProto() {
        if (project.isPresent()) {
            return com.dimajix.flowman.kernel.proto.ConnectionIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.ConnectionIdentifier.newBuilder()
                .setName(name)
                .build();
        }
    }
}
