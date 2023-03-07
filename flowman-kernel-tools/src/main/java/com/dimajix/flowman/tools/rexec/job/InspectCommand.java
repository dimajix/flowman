/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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

import java.util.Comparator;
import java.util.Map;

import lombok.val;
import org.slf4j.Logger;

import org.kohsuke.args4j.Argument;
import org.slf4j.LoggerFactory;

import static com.dimajix.common.ExceptionUtils.isFatal;
import static com.dimajix.common.ExceptionUtils.reasons;

import com.dimajix.common.ExceptionUtils;
import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Job;
import com.dimajix.flowman.kernel.model.JobIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(InspectCommand.class);

    @Argument(index=0, required=true, usage = "name of job to inspect", metaVar = "<job>")
    String job = "";

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        try {
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
        catch (Throwable e) {
            if (isFatal(e))
                throw e;
            logger.error("Error inspecting job '" + job + "':\n  " + reasons(e));
            return Status.FAILED;
        }
    }
}
