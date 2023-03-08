/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.flowman.tools.exec.history

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.NestedCommand


class JobHistoryCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="inspect",impl=classOf[InspectJobHistoryCommand]),
        new SubCommand(name="search",impl=classOf[SearchJobHistoryCommand])
    ))
    override var command:Command = _
}
