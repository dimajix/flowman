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

package com.dimajix.flowman.tools.rexec.job;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.dimajix.flowman.tools.rexec.Command;
import com.dimajix.flowman.tools.rexec.NestedCommand;


public class JobCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=SubCommandHandler.class)
    @SubCommands({
        @SubCommand(name = "inspect", impl = InspectCommand.class),
        @SubCommand(name = "list", impl = ListCommand.class),
        @SubCommand(name = "validate", impl = ValidateCommand.class),
        @SubCommand(name = "create", impl = CreateCommand.class),
        @SubCommand(name = "build", impl = BuildCommand.class),
        @SubCommand(name = "verify", impl = VerifyCommand.class),
        @SubCommand(name = "truncate", impl = TruncateCommand.class),
        @SubCommand(name = "destroy", impl = DestroyCommand.class)
    })
    Command command = null;

    @Override
    public Command getCommand() { return command; }
}
