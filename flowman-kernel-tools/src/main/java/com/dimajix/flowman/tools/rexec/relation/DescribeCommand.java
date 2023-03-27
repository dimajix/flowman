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

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import static com.dimajix.common.text.ParserUtils.parseDelimitedKeyValues;


class DescribeCommand extends Command {
    @Option(name = "-s", aliases={"--spark"}, usage = "use Spark to derive final schema")
    boolean useSpark = false;
    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    String relation = "";
    @Option(name="-p", aliases={"--partition"}, usage = "specify partition to work on, as partition1=value1,partition2=value2")
    String partition = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val identifier = RelationIdentifier.ofString(this.relation);
        val partition = parseDelimitedKeyValues(this.partition);
        val schema = session.describeRelation(identifier, partition, useSpark);
        schema.printTree();
        return Status.SUCCESS;
    }
}
