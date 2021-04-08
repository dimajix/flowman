/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.mapping

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.NestedCommand


class MappingCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="count",impl=classOf[CountCommand]),
        new SubCommand(name="describe",impl=classOf[DescribeCommand]),
        new SubCommand(name="explain",impl=classOf[ExplainCommand]),
        new SubCommand(name="deptree",impl=classOf[DependencyTreeCommand]),
        new SubCommand(name="inspect",impl=classOf[InspectCommand]),
        new SubCommand(name="list",impl=classOf[ListCommand]),
        new SubCommand(name="validate",impl=classOf[ValidateCommand]),
        new SubCommand(name="export-schema",impl=classOf[ExportSchemaCommand]),
        new SubCommand(name="save",impl=classOf[SaveCommand]),
        new SubCommand(name="show",impl=classOf[ShowCommand])
    ))
    override var command:Command = _
}
