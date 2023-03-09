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

package com.dimajix.flowman.tools.rexec.test;

import java.util.Map;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dimajix.common.ExceptionUtils.isFatal;
import static com.dimajix.common.ExceptionUtils.reasons;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.MappingIdentifier;
import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.kernel.model.TestIdentifier;
import com.dimajix.flowman.tools.rexec.Command;


class InspectCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(InspectCommand.class);

    @Argument(index=0, required=true, usage = "name of test to inspect", metaVar = "<test>")
    String test = "";

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        try {
            val test = session.getTest(TestIdentifier.ofString(this.test));
            System.out.println("Name: " + test.getName());
            System.out.println("Description: " + test.getDescription().orElse(""));

            System.out.println("Environment:");
            test.getEnvironment()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));
            System.out.println("Mapping Overrides:");
            test.getOverrideMappings()
                .stream()
                .map(MappingIdentifier::toString)
                .sorted()
                .forEach(p -> System.out.println("    " + p));
            System.out.println("Relation Overrides:");
            test.getOverrideRelations()
                .stream()
                .map(RelationIdentifier::toString)
                .sorted()
                .forEach(p -> System.out.println("    " + p));
            System.out.println("Fixture Targets:");
            test.getFixtureTargets()
                .stream()
                .map(TargetIdentifier::toString)
                .sorted()
                .forEach(p -> System.out.println("    " + p));
            System.out.println("Build Targets:");
            test.getBuildTargets()
                .stream()
                .map(TargetIdentifier::toString)
                .sorted()
                .forEach(p -> System.out.println("    " + p));
            System.out.println("Assertions:");
            test.getAssertions()
                .stream()
                .sorted()
                .forEach(p -> System.out.println("    " + p));
            return Status.SUCCESS;
        }
        catch (Throwable e) {
            if (isFatal(e))
                throw e;
            logger.error("Error inspecting test '" + test + "':\n  "+ reasons(e));
            return Status.FAILED;
        }
    }
}