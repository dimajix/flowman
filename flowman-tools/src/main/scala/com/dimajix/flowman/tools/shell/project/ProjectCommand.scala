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

package com.dimajix.flowman.tools.shell.project

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.NestedCommand
import com.dimajix.flowman.tools.exec.project.BuildCommand
import com.dimajix.flowman.tools.exec.project.CreateCommand
import com.dimajix.flowman.tools.exec.project.DestroyCommand
import com.dimajix.flowman.tools.exec.project.InspectCommand
import com.dimajix.flowman.tools.exec.project.TruncateCommand
import com.dimajix.flowman.tools.exec.project.ValidateCommand
import com.dimajix.flowman.tools.exec.project.VerifyCommand


class ProjectCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="validate",impl=classOf[ValidateCommand]),
        new SubCommand(name="create",impl=classOf[CreateCommand]),
        new SubCommand(name="migrate",impl=classOf[CreateCommand]),
        new SubCommand(name="build",impl=classOf[BuildCommand]),
        new SubCommand(name="verify",impl=classOf[VerifyCommand]),
        new SubCommand(name="truncate",impl=classOf[TruncateCommand]),
        new SubCommand(name="destroy",impl=classOf[DestroyCommand]),
        new SubCommand(name="inspect",impl=classOf[InspectCommand]),
        new SubCommand(name="load",impl=classOf[LoadCommand]),
        new SubCommand(name="reload",impl=classOf[ReloadCommand])
    ))
    override var command:Command = _
}
