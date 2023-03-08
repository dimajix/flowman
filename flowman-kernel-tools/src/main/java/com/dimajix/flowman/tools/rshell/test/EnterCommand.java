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

package com.dimajix.flowman.tools.rshell.test;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dimajix.common.ExceptionUtils.isFatal;
import static com.dimajix.common.ExceptionUtils.reasons;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


class EnterCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(EnterCommand.class);

    @Argument(index=0, required=true, usage = "name of test to enter", metaVar = "<test>")
    String test = "";

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        try {
            session.enterTestContext(test);
            return Status.SUCCESS;
        }
        catch (Throwable e) {
            if (isFatal(e))
                throw e;
            logger.error("Error entering test '" + test + "':\n  "+ reasons(e));
            return Status.FAILED;
        }
    }
}
