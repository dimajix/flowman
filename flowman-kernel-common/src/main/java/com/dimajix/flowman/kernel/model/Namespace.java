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

import lombok.Value;


@Value
public class Namespace {
    public static Namespace ofProto(com.dimajix.flowman.kernel.proto.kernel.NamespaceDetails namespace) {
        return new Namespace(
            namespace.getName(),
            namespace.getPluginsList(),
            namespace.getEnvironmentMap(),
            namespace.getConfigMap(),
            namespace.getProfilesList(),
            namespace.getConnectionsList()
        );
    }

    String name;
    List<String> plugins;
    Map<String,String> environment;
    Map<String,String> config ;
    List<String> profiles;
    List<String> connections;
}
