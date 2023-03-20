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


public class InspectTargetHistoryCommand extends Command {
    @Argument(usage = "Target run ID", metaVar = "<target_run_id>", required = true)
    String targetId = "";

    @Override
    public Status execute(ExecutionContext context) {
        val history = context.getHistory();
        val firstTarget = history.getTarget(targetId);
        if (firstTarget.isPresent()) {
            val target = firstTarget.get();
            System.out.println("Target run id: " + target.getId());
            System.out.println("  Namespace: " + target.getNamespace());
            System.out.println("  Project: " + target.getProject());
            System.out.println("  Project version: " + target.getVersion());
            System.out.println("  Target name: " + target.getTarget());
            System.out.println("  Phase: " + target.getPhase().toString());
            System.out.println("  Status: " + target.getStatus().toString());
            System.out.println("  Start date: " + target.getStartDateTime().map(ZonedDateTime::toString).orElse(""));
            System.out.println("  End date: " + target.getEndDateTime().map(ZonedDateTime::toString).orElse(""));
            System.out.println("  Error: " + target.getError().orElse(""));

            System.out.println("Target partitions:");
            val args = target.getPartitions().entrySet().stream()
                .sorted()
                .collect(Collectors.toList());
            for (val a : args) {
                System.out.println("  " + a.getKey() + " = " + a.getValue());
            }
            return Status.SUCCESS;
        }
        else {
            return Status.FAILED;
        }
    }
}
