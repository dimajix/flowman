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

package com.dimajix.flowman.tools.rexec.relation;

import lombok.val;
import org.kohsuke.args4j.Argument;

import com.dimajix.flowman.kernel.model.Operation;
import com.dimajix.flowman.kernel.model.Relation;
import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;


class InspectCommand extends Command {
    @Argument(required = true, usage = "specifies relation to inspect", metaVar = "<relation>")
    String relation = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val relation = session.getRelation(RelationIdentifier.ofString(this.relation));
        System.out.println("Relation:");
        System.out.println("    name: " + relation.getName());
        System.out.println("    kind: " + relation.getKind());
        printDependencies(relation, Operation.CREATE);
        printDependencies(relation, Operation.READ);
        printDependencies(relation, Operation.WRITE);
        return Status.SUCCESS;
    }
    private void printDependencies(Relation relation, Operation op) {
        System.out.println("  Requires - " + op + ":");
        val req = relation.getRequires();
        if (req.containsKey(op)) {
            req.get(op)
                .stream()
                .sorted()
                .forEach(p -> System.out.println("    " + p));
        }

        System.out.println("  Provides - $op:");
        val prov = relation.getProvides();
        if (prov.containsKey(op)) {
            prov.get(op)
                .stream()
                .sorted()
                .forEach(p -> System.out.println("    " + p));
        }
    }
}
