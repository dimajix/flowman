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

package com.dimajix.flowman.tools.rexec.test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.kernel.model.TestIdentifier;
import com.dimajix.flowman.tools.rexec.Command;


public class RunCommand extends Command {
    @Argument(required = false, usage = "specifies tests(s) to execute", metaVar = "<tests>")
    String[] tests = new String[0];
    @Option(name = "-k", aliases={"--keep-going"}, usage = "continues execution of all tests in case of errors")
    boolean keepGoing = false;
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    boolean dryRun = false;
    @Option(name = "-j", aliases={"--jobs"}, usage = "number of tests to run in parallel")
    int parallelism = 1;

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        List<TestIdentifier> allTests = Arrays.stream(tests)
            .flatMap(t -> Arrays.stream(t.split(",")))
            .map(TestIdentifier::ofString)
            .collect(Collectors.toList());

        if (allTests.isEmpty()) {
            allTests = session.listTests();
        }

        return session.executeTests(allTests, keepGoing, dryRun, parallelism);
    }
}
