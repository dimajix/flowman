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
import java.util.Optional;

import lombok.Value;


@Value
public class Kernel {
    public static Kernel ofProto(com.dimajix.flowman.kernel.proto.kernel.KernelDetails kernel) {
        return new Kernel(
            kernel.hasFlowmanHomeDirectory() ? Optional.of(kernel.getFlowmanHomeDirectory()) : Optional.empty(),
            kernel.hasFlowmanConfigDirectory() ? Optional.of(kernel.getFlowmanConfigDirectory()) : Optional.empty(),
            kernel.hasFlowmanPluginDirectory() ? Optional.of(kernel.getFlowmanPluginDirectory()) : Optional.empty(),
            kernel.getFlowmanVersion(),
            kernel.getSparkVersion(),
            kernel.getHadoopVersion(),
            kernel.getJavaVersion(),
            kernel.getScalaVersion(),
            kernel.getSparkBuildVersion(),
            kernel.getHadoopBuildVersion(),
            kernel.getScalaBuildVersion(),
            kernel.getActivePluginsList()
        );
    }

    Optional<String> flowmanHomeDirectory;
    Optional<String> flowmanConfigDirectory;
    Optional<String> flowmanPluginDirectory;
    String flowmanVersion;
    String sparkVersion;
    String hadoopVersion;
    String javaVersion;
    String scalaVersion;
    String sparkBuildVersion;
    String hadoopBuildVersion;
    String scalaBuildVersion;
    List<String> activePlugins;
}
