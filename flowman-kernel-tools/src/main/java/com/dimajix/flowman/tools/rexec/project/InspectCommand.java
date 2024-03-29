/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.flowman.tools.rexec.project;

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

        System.out.println("Environment:");
        project.getEnvironment()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Configuration:");
        project.getConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));
        System.out.println("Profiles:");
        project.getProfiles()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Mappings:");
        project.getMappings()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Relations:");
        project.getRelations()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Jobs:");
        project.getJobs()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Targets:");
        project.getTargets()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Tests:");
        project.getTests()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));
        return Status.SUCCESS;
    }
}
