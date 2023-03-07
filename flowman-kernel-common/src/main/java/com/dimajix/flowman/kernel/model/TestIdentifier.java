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
public class TestIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        if (project.isPresent())
            return project.get() + "/" + name;
        else
            return name;
    }

    public static TestIdentifier ofString(String testId) {
        val split = testId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = testId.substring(0, split);
            val lastPart = testId.substring(split + 1);
            return new TestIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new TestIdentifier(testId, Optional.empty());
        }
    }

    public static TestIdentifier ofProto(com.dimajix.flowman.kernel.proto.TestIdentifier testId) {
        if (testId.hasProject()) {
            return new TestIdentifier(testId.getName(), Optional.of(testId.getProject()));
        }
        else {
            return new TestIdentifier(testId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.TestIdentifier toProto() {
        if (project.isEmpty()) {
            return com.dimajix.flowman.kernel.proto.TestIdentifier.newBuilder()
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.TestIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
    }
}
