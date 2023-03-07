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
public class JobIdentifier {
    String name;
    Optional<String> project;

    @Override
    public String toString() {
        return project.map(s -> s + "/" + name).orElse(name);
    }

    public static JobIdentifier ofString(String jobId) {
        val split = jobId.lastIndexOf("/");
        if (split >= 0) {
            val firstPart = jobId.substring(0, split);
            val lastPart = jobId.substring(split + 1);
            return new JobIdentifier(lastPart, Optional.of(firstPart));
        }
        else {
            return new JobIdentifier(jobId, Optional.empty());
        }
    }

    public static JobIdentifier ofProto(com.dimajix.flowman.kernel.proto.JobIdentifier jobId) {
        if (jobId.hasProject()) {
            return new JobIdentifier(jobId.getName(), Optional.of(jobId.getProject()));
        }
        else {
            return new JobIdentifier(jobId.getName(), Optional.empty());
        }
    }

    public com.dimajix.flowman.kernel.proto.JobIdentifier toProto() {
        if (project.isPresent()) {
            return com.dimajix.flowman.kernel.proto.JobIdentifier.newBuilder()
                .setProject(project.get())
                .setName(name)
                .build();
        }
        else {
            return com.dimajix.flowman.kernel.proto.JobIdentifier.newBuilder()
                .setName(name)
                .build();
        }
    }
}
