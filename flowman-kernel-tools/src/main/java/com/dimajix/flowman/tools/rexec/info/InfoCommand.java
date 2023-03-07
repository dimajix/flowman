/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.rexec.info;

import java.util.Map;

import lombok.val;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class InfoCommand extends Command {
    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val info = kernel.getKernel();
        System.out.println("Flowman kernel server:");
        System.out.println("    version: " + info.getFlowmanVersion());
        System.out.println("    Java version: " + info.getJavaVersion());
        System.out.println("    Scala version: " + info.getScalaVersion() + " (execution) / " + info.getScalaBuildVersion() + " (build)");
        System.out.println("    Spark version: " + info.getSparkVersion() + " (execution) / " + info.getSparkBuildVersion() + " (build)");
        System.out.println("    Hadoop version: " + info.getHadoopVersion() + " (execution) / " + info.getHadoopBuildVersion() + " (build)");
        System.out.println("    home directory: " + info.getFlowmanHomeDirectory().orElse("<unknown>"));
        System.out.println("    config directory: " + info.getFlowmanConfigDirectory().orElse("<unknown>"));
        System.out.println("    plugin directory: " + info.getFlowmanPluginDirectory().orElse("<unknown>"));

        val namespace = kernel.getNamespace();
        System.out.println("Namespace:");
        System.out.println("    name: " + namespace.getName());
        System.out.println("    plugins: " + namespace.getPlugins().stream().reduce((l,r) -> l + "," + r));

        val project = session.getProject();
        System.out.println("Project:");
        System.out.println("    name: " + project.getName());
        System.out.println("    version: " + project.getVersion().orElse(""));
        System.out.println("    description: " + project.getDescription().orElse(""));
        System.out.println("    basedir: " + project.getBasedir().orElse(""));
        System.out.println("    filename: " + project.getFilename().orElse(""));

        val context = session.getSession();
        System.out.println("Active Profiles:");
        context.getProfiles()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));

        System.out.println("Environment:");
        context.getEnvironment()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Configuration:");
        context.getConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Flowman Configuration:");
        context.getFlowmanConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Spark Configuration:");
        context.getSparkConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Hadoop Configuration:");
        context.getHadoopConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        return Status.SUCCESS;
    }
}
