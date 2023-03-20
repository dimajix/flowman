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

package com.dimajix.flowman.tools.rshell.project;

import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import com.dimajix.flowman.tools.rshell.Shell;
import org.kohsuke.args4j.Argument;


public class LoadCommand extends Command {
    @Argument(index=0, required=true, usage = "name of project to load", metaVar = "<project>")
    String project = "";

    @Override
    public Status execute(ExecutionContext context) {
        Shell.getInstance().newSession(this.project);
        return Status.SUCCESS;
    }
}
