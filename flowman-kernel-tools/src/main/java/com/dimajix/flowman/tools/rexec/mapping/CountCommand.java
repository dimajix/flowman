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

import com.dimajix.flowman.kernel.model.MappingOutputIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


public class CountCommand extends Command {
    @Argument(usage = "specifies the mapping to count", metaVar = "<mapping>", required = true)
    String mapping = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val identifier = MappingOutputIdentifier.ofString(this.mapping);

        val numRecords = session.countMapping(identifier);
        System.out.println("Mapping '" + identifier.getMapping() + "' contains " + numRecords + " records");
        return Status.SUCCESS;
    }
}
