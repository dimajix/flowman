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

package com.dimajix.flowman.tools.rexec.target;

import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.val;
import org.slf4j.Logger;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.LoggerFactory;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Lifecycle;
import com.dimajix.flowman.kernel.model.Phase;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


class PhaseCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(PhaseCommand.class);

    @Argument(required = true, usage = "specifies target(s) to execute", metaVar = "<target>")
    String[] targets = new String[0];
    @Option(name = "-f", aliases= {"--force"}, usage = "forces execution, even if outputs are already created")
    boolean force = false;
    @Option(name = "-k", aliases={"--keep-going"}, usage = "continues execution of job with next target in case of errors")
    boolean keepGoing = false;
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    boolean dryRun = false;
    @Option(name = "-nl", aliases={"--no-lifecycle"}, usage = "only executes the specific phase and not the whole lifecycle")
    boolean noLifecycle = false;

    private final Phase phase;

    protected PhaseCommand(Phase phase) {
        this.phase = phase;
    }

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val lifecycle = noLifecycle ? Arrays.asList(phase) : Lifecycle.ofPhase(phase).phases;

        val allTargets = Arrays.stream(targets)
            .flatMap(t -> Arrays.stream(t.split(",")))
            .map(TargetIdentifier::ofString)
            .collect(Collectors.toList());

        return session.executeTargets(allTargets, lifecycle, force, keepGoing, dryRun);
    }
}
