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
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.common.ParserUtils;
import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.MappingOutputIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class ShowCommand extends Command {
    @Argument(index=0, usage="Specifies the mapping to show", metaVar="<mapping>", required=true)
    String mapping = "";
    @Argument(index=1, usage="Specifies the columns to show as a comma separated list", metaVar="<columns>", required=false)
    String columns = "";
    @Option(name="-n", aliases={"--limit"}, usage="Specifies maximum number of rows to print", metaVar="<limit>", required = false)
    int limit = 10;


    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val columns = ParserUtils.parseDelimitedList(this.columns);
        val identifier = MappingOutputIdentifier.ofString(this.mapping);

        val df = session.readMapping(identifier.getMapping(), identifier.getOutput(), columns, limit);
        df.show();
        return Status.SUCCESS;
    }
}
