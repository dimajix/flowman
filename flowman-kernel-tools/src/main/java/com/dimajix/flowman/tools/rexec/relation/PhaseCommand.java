/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dimajix.flowman.kernel.model.Phase;
import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import static com.dimajix.common.text.ParserUtils.parseDelimitedKeyValues;


public class PhaseCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Argument(required=true, usage = "specifies relations to execute", metaVar = "<relation>")
    String[] relations = new String[0];
    @Option(name = "-f", aliases= {"--force"}, usage = "forces execution, even if outputs are already created")
    boolean force = false;
    @Option(name = "-k", aliases={"--keep-going"}, usage = "continues execution of job with next target in case of errors")
    boolean keepGoing = false;
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    boolean dryRun = false;
    @Option(name = "-p", aliases={"--partition"}, usage = "specify partition to work on, as partition1=value1,partition2=value2")
    String partition = "";

    private final Phase phase;

    protected PhaseCommand(Phase phase) {
        this.phase = phase;
    }

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        logger.info("Executing phase '" + phase + "' for relations " + Arrays.stream(relations).reduce((k,v) -> k + "," + v));

        val toRun = Arrays.stream(relations)
            .flatMap(r -> Arrays.stream(r.split(",")))
            .map(RelationIdentifier::ofString)
            .collect(Collectors.toList());
        val partition = parseDelimitedKeyValues(this.partition);

        return session.executeRelations(toRun, phase, partition, force, keepGoing, dryRun);
    }
}
