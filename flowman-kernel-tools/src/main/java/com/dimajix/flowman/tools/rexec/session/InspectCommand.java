/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.tools.rexec.session;

import java.util.Map;

import lombok.val;

import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val project = session.getProject();
        System.out.println("Project:");
        System.out.println("    name: " + project.getName());
        System.out.println("    version: " + project.getVersion().orElse(""));
        System.out.println("    description: " + project.getDescription().orElse(""));
        System.out.println("    basedir: " + project.getBasedir().orElse(""));
        System.out.println("    filename: " + project.getFilename().orElse(""));

        val sesn = session.getSession();
        System.out.println("Active Profiles:");
        sesn.getProfiles()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));

        System.out.println("Active Environment:");
        sesn.getEnvironment()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Active Configuration:");
        sesn.getConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Flowman Configuration:");
        sesn.getFlowmanConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Spark Configuration:");
        sesn.getSparkConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Hadoop Configuration:");
        sesn.getHadoopConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        return Status.SUCCESS;
    }
}
