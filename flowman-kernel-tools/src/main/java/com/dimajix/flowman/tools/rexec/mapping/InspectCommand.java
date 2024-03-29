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

package com.dimajix.flowman.tools.rexec.mapping;

import lombok.val;
import org.kohsuke.args4j.Argument;

import com.dimajix.flowman.kernel.model.MappingIdentifier;
import com.dimajix.flowman.kernel.model.MappingOutputIdentifier;
import com.dimajix.flowman.kernel.model.ResourceIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    @Argument(required = true, usage = "specifies mapping to inspect", metaVar = "<mapping>")
    String mapping = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val mapping = session.getMapping(MappingIdentifier.ofString(this.mapping));
        System.out.println("Mapping:");
        System.out.println("    name: " + mapping.getName());
        System.out.println("    kind: " + mapping.getKind());
        System.out.println("    inputs: " + mapping.getInputs().stream().map(MappingOutputIdentifier::toString).reduce((l,r) -> l + "," + r));
        System.out.println("    outputs: " + mapping.getOutputs().stream().reduce((l,r) -> l + "," + r));
        System.out.println("    cache: " + mapping.getCache());
        System.out.println("    broadcast: " + mapping.isBroadcast());
        System.out.println("    checkpoint: " + mapping.isCheckpoint());
        System.out.println("  Requires:");
            mapping.getRequired()
                .stream()
                .map(ResourceIdentifier::toString)
                .sorted()
                .forEach(p -> System.out.println("    " + p));
        return Status.SUCCESS;
    }
}
