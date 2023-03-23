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

package com.dimajix.flowman.tools.rexec.history;

import java.time.ZonedDateTime;
import java.util.stream.Collectors;

import lombok.val;
import org.kohsuke.args4j.Argument;

import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectJobHistoryCommand extends Command {
    @Argument(usage = "Job run ID", metaVar = "<job_run_id>", required = true)
    String jobId = "";

    @Override
    public Status execute(ExecutionContext context) {
        val history = context.getHistory();
        val firstJob = history.getJob(jobId);
        if (firstJob.isPresent()) {
            val job = firstJob.get();
            System.out.println("Job run id: " + job.getId());
            System.out.println("  Namespace: " + job.getNamespace());
            System.out.println("  Project: " + job.getProject());
            System.out.println("  Project version: " + job.getVersion());
            System.out.println("  Job name: " + job.getJob());
            System.out.println("  Phase: " + job.getPhase().toString());
            System.out.println("  Status: " + job.getStatus().toString());
            System.out.println("  Start date: " + job.getStartDateTime().map(ZonedDateTime::toString).orElse(""));
            System.out.println("  End date: " + job.getEndDateTime().map(ZonedDateTime::toString).orElse(""));
            System.out.println("  Error: " + job.getError().orElse(""));

            System.out.println("Job arguments:");
            val args = job.getArguments().entrySet().stream()
                .sorted()
                .collect(Collectors.toList());
            for (val a : args) {
                System.out.println("  " + a.getKey() + " = " + a.getValue());
            }

            val metrics = history.getJobMetrics(job.getId());
            System.out.println("Job execution metrics:");
            for (val m : metrics) {
                val labels = m.getLabels().entrySet().stream()
                    .sorted()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .reduce((l,r) -> l + "," + r)
                    .orElse("");

                System.out.println("  " + m.getName() + " ts=" + m.getTs().toString() + " labels=" + labels + " value=" + m.getValue() );
            }
            return Status.SUCCESS;
        }
        else {
            return Status.FAILED;
        }
    }
}
