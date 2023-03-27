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

package com.dimajix.flowman.tools.rexec.relation;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import static com.dimajix.common.text.ParserUtils.parseDelimitedKeyValues;
import static com.dimajix.common.text.ParserUtils.parseDelimitedList;


public class ShowCommand extends Command {
    @Argument(index=0, usage="Specifies the relation to show", metaVar="<relation>", required=true)
    String relation = "";
    @Argument(index=1, usage="Specifies the columns to show as a comma separated list", metaVar="<columns>", required=false)
    String columns = "";
    @Option(name="-n", aliases={"--limit"}, usage="Specifies maximum number of rows to print", metaVar="<limit>", required = false)
    int limit = 10;
    @Option(name="-p", aliases={"--partition"}, usage = "specify partition to work on, as partition1=value1,partition2=value2")
    String partition = "";


    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val columns = parseDelimitedList(this.columns);
        val identifier = RelationIdentifier.ofString(this.relation);
        val partition = parseDelimitedKeyValues(this.partition);

        val df = session.readRelation(identifier, partition, columns, limit);
        df.show();
        return Status.SUCCESS;
    }
}
