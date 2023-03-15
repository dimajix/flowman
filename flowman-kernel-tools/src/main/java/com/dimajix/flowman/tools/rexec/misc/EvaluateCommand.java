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

import java.util.Arrays;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.spi.RestOfArgumentsHandler;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class EvaluateCommand extends Command {
    @Argument(index=0, required=true, usage = "expression to evaluate", metaVar = "<expr>", handler=RestOfArgumentsHandler.class)
    String[] expression = new String[0];

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val expr = Arrays.stream(expression).reduce((l, r) -> l + " " + r).orElse("");
        val result = session.evaluateExpression(expr);
        System.out.println(result);
        return Status.SUCCESS;
    }
}
