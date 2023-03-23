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

package com.dimajix.flowman.tools.rexec.job;

import com.dimajix.flowman.kernel.model.Job;
import com.dimajix.flowman.kernel.model.JobIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import lombok.val;
import org.kohsuke.args4j.Argument;

import java.util.Comparator;
import java.util.Map;


public class InspectCommand extends Command {
    @Argument(index=0, required=true, usage = "name of job to inspect", metaVar = "<job>")
    String job = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val job = session.getJob(JobIdentifier.ofString(this.job));
        System.out.println("Name: " + job.getName());
        System.out.println("Description: " + job.getDescription());
        System.out.println("Targets:");
        job.getTargets()
            .forEach(p -> System.out.println("    " + p));
        System.out.println("Parameters:");
        job.getParameters()
            .stream()
            .sorted(Comparator.comparing(Job.Parameter::getName))
            .forEach(p -> System.out.println("    " + p.getName() + " : " + p.getType()));
        System.out.println("Environment:");
        job.getEnvironment()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));
        return Status.SUCCESS;
    }
}
