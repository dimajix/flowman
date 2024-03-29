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

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.dimajix.flowman.tools.rexec.Command;
import com.dimajix.flowman.tools.rexec.NestedCommand;


public class MappingCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=SubCommandHandler.class)
    @SubCommands({
        @SubCommand(name = "cache", impl = CacheCommand.class),
        @SubCommand(name = "count", impl = CountCommand.class),
        @SubCommand(name = "describe", impl = DescribeCommand.class),
        @SubCommand(name = "explain", impl = ExplainCommand.class),
        @SubCommand(name = "inspect", impl = InspectCommand.class),
        @SubCommand(name = "list", impl = ListCommand.class),
        @SubCommand(name = "save", impl = SaveCommand.class),
        @SubCommand(name = "show", impl = ShowCommand.class),
        @SubCommand(name = "validate", impl = ValidateCommand.class)
    })
    Command command = null;

    @Override
    public Command getCommand() { return command; }
}
