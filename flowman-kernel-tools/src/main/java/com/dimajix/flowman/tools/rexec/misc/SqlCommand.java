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

package com.dimajix.flowman.tools.rexec.misc;

import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.RestOfArgumentsHandler;

import java.util.Arrays;


public class SqlCommand extends Command {
    @Option(name="-n", aliases={"--limit"}, usage="Specifies maximum number of rows to print", metaVar="<limit>", required = false)
    int limit = 100;
    @Argument(index = 0, required = true, usage = "SQL statement to execute", metaVar = "<sql>", handler = RestOfArgumentsHandler.class)
    String[] statement = new String[0];

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val sql = Arrays.stream(statement).reduce((l,r) -> l + "\n" + r).orElse("");
        val df = session.executeSql(sql, limit);
        df.show();
        return Status.SUCCESS;
    }
}
