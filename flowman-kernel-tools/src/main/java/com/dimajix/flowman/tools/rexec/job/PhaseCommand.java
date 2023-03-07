/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import java.util.Arrays;

import lombok.val;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import static com.dimajix.flowman.common.ParserUtils.splitSettings;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.JobIdentifier;
import com.dimajix.flowman.kernel.model.Lifecycle;
import com.dimajix.flowman.kernel.model.Phase;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


class PhaseCommand extends Command {
    @Argument(index=0, required=true, usage = "specifies job to run", metaVar = "<job>")
    String job = "";
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    String[] args = new String[0];
    @Option(name = "-t", aliases={"--target"}, usage = "only process specific targets, as specified by a regex", metaVar = "<target>")
    String[] targets = new String[]{".*"};
    @Option(name = "-d", aliases= {"--dirty"}, usage = "mark targets as being dirty, as specified by a regex", metaVar = "<target>")
    String[] dirtyTargets = new String[0];
    @Option(name = "-f", aliases= {"--force"}, usage = "forces execution, even if outputs are already created")
    boolean force = false;
    @Option(name = "-k", aliases={"--keep-going"}, usage = "continues execution of job with next target in case of errors")
    boolean keepGoing = false;
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    boolean dryRun = false;
    @Option(name = "-nl", aliases={"--no-lifecycle"}, usage = "only executes the specific phase and not the whole lifecycle")
    boolean noLifecycle = false;
    @Option(name = "-j", aliases={"--jobs"}, usage = "number of jobs to run in parallel")
    int parallelism = 1;

    private final Phase phase;

    protected PhaseCommand(Phase phase) {
        this.phase = phase;
    }

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val args = splitSettings(this.args);
        val lifecycle = noLifecycle ? Arrays.asList(phase) : Lifecycle.ofPhase(phase).phases;

        val jobId = JobIdentifier.ofString(job);
        return session.executeJob(jobId, lifecycle, args, Arrays.asList(targets), Arrays.asList(dirtyTargets), force, keepGoing, dryRun, parallelism);
    }
}
