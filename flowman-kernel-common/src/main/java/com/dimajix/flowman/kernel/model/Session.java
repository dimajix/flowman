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

import lombok.Value;


@Value
public class Session {
    public static Session ofProto(com.dimajix.flowman.kernel.proto.session.SessionDetails session) {
        return new Session(
            session.getId(),
            session.hasName() ? Optional.of(session.getName()) : Optional.empty(),
            session.hasWorkspace() ? Optional.of(session.getWorkspace()) : Optional.empty(),
            session.hasNamespace() ? Optional.of(session.getNamespace()) : Optional.empty(),
            session.hasProject() ? Optional.of(session.getProject()) : Optional.empty(),
            session.hasProjectVersion() ? Optional.of(session.getProjectVersion()) : Optional.empty(),
            session.hasProjectDescription() ? Optional.of(session.getProjectDescription()) : Optional.empty(),
            session.hasProjectBasedir() ? Optional.of(session.getProjectBasedir()) : Optional.empty(),
            session.hasProjectFilename() ? Optional.of(session.getProjectFilename()) : Optional.empty(),
            session.getProfilesList(),
            session.getEnvironmentMap(),
            session.getConfigMap(),
            session.getFlowmanConfigMap(),
            session.getHadoopConfigMap(),
            session.getSparkConfigMap()
        );
    }

    String id;
    Optional<String> name;
    Optional<String> workspace;
    Optional<String> namespace;
    Optional<String> project;
    Optional<String> projectVersion;
    Optional<String> projectDescription;
    Optional<String> projectBasedir;
    Optional<String> projectFilename;
    List<String> profiles;
    Map<String,String> environment;
    Map<String,String> config;
    Map<String,String> flowmanConfig;
    Map<String,String> hadoopConfig;
    Map<String,String> sparkConfig;
}
