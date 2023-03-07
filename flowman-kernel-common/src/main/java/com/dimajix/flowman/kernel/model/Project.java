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


import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Value;


@Value
public class Project {
    public static Project ofProto(com.dimajix.flowman.kernel.proto.project.ProjectDetails project) {
        return new Project(
            project.getName(),
            project.hasVersion() ? Optional.of(project.getVersion()) : Optional.empty(),
            project.hasBasedir() ? Optional.of(project.getBasedir()) : Optional.empty(),
            project.hasFilename() ? Optional.of(project.getFilename()) : Optional.empty(),
            project.hasDescription() ? Optional.of(project.getDescription()) : Optional.empty(),
            project.getTargetsList(),
            project.getTestsList(),
            project.getJobsList(),
            project.getMappingsList(),
            project.getRelationsList(),
            project.getConnectionsList(),
            project.getEnvironmentMap(),
            project.getConfigMap(),
            project.getProfilesList()
        );
    }

    String name;
    Optional<String> version;
    Optional<String> basedir;
    Optional<String> filename;
    Optional<String> description;
    List<String> targets;
    List<String> tests;
    List<String> jobs;
    List<String> mappings;
    List<String> relations;
    List<String> connections;
    Map<String,String> environment;
    Map<String,String> config;
    List<String> profiles;
}
