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

import com.dimajix.flowman.kernel.model.MappingIdentifier;
import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.kernel.model.TestIdentifier;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    @Argument(index=0, required=true, usage = "name of test to inspect", metaVar = "<test>")
    String test = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
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
}
