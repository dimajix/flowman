/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.tools.rexec.target;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dimajix.common.ExceptionUtils.isFatal;
import static com.dimajix.common.ExceptionUtils.reasons;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Lifecycle;
import com.dimajix.flowman.kernel.model.Phase;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.kernel.model.Target;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(InspectCommand.class);

    @Argument(required = true, usage = "specifies target to inspect", metaVar = "<target>")
    String target = "";

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        try {
            val target = session.getTarget(TargetIdentifier.ofString(this.target));
            System.out.println("Target:");
            System.out.println("    name: " + target.getName());
            System.out.println("    kind: " + target.getKind());
            System.out.println("    phases: " + target.getPhases().stream().map(Phase::toString).reduce((k, v) -> k + "," + v));
            System.out.println("    before: " + target.getBefore().stream().map(TargetIdentifier::toString).reduce((k, v) -> k + "," + v));
            System.out.println("    after: " + target.getAfter().stream().map(TargetIdentifier::toString).reduce((k, v) -> k + "," + v));
            Lifecycle.ALL.phases.forEach(p -> printDependencies(target,p));
            return Status.SUCCESS;
        }
        catch (Throwable e) {
            if (isFatal(e))
                throw e;
            logger.error("Error inspecting target '" + target + "':\n  "+ reasons(e));
            return Status.FAILED;
        }
    }

    private void printDependencies(Target target, Phase phase) {
        System.out.println("Phase '" + phase + (!target.getPhases().contains(phase) ? " (inactive)" : ""));
        System.out.println("  Requires:");
        val req = target.getRequires();
        if (req.containsKey(phase)) {
            req.get(phase)
                .stream()
                .sorted()
                .forEach(p -> System.out.println("    " + p));
        }

        System.out.println("  Provides:");
        val prov = target.getProvides();
        if (prov.containsKey(phase)) {
            prov.get(phase)
                .stream()
                .sorted()
                .forEach(p -> System.out.println("    " + p));
        }
    }
}