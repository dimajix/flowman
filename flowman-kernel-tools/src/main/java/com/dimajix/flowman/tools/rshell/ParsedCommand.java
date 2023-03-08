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

package com.dimajix.flowman.tools.rshell;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.dimajix.flowman.tools.rexec.Command;
import com.dimajix.flowman.tools.rexec.info.InfoCommand;
import com.dimajix.flowman.tools.rexec.mapping.MappingCommand;
import com.dimajix.flowman.tools.rexec.namespace.NamespaceCommand;
import com.dimajix.flowman.tools.rexec.project.ProjectCommand;
import com.dimajix.flowman.tools.rexec.relation.RelationCommand;
import com.dimajix.flowman.tools.rexec.target.TargetCommand;
import com.dimajix.flowman.tools.rshell.job.JobCommand;
import com.dimajix.flowman.tools.rshell.test.TestCommand;


public class ParsedCommand {
    @Argument(required=false,index=0,metaVar="<command-group>",usage="the object to work with",handler=SubCommandHandler.class)
    @SubCommands({
        //new SubCommand(name="documentation",impl=classOf[DocumentationCommand]),
        //new SubCommand(name="eval",impl=classOf[EvaluateCommand]),
        @SubCommand(name = "exit", impl = ExitCommand.class),
        //new SubCommand(name="history",impl=classOf[HistoryCommand]),
        @SubCommand(name = "info", impl = InfoCommand.class),
        @SubCommand(name = "job", impl = JobCommand.class),
        @SubCommand(name = "mapping", impl = MappingCommand.class),
        @SubCommand(name = "model", impl = RelationCommand.class),
        @SubCommand(name = "namespace", impl = NamespaceCommand.class),
        @SubCommand(name = "project", impl = ProjectCommand.class),
        @SubCommand(name = "quit", impl = ExitCommand.class),
        @SubCommand(name = "relation", impl = RelationCommand.class),
        //new SubCommand(name="sql",impl=SqlCommand.class),
        @SubCommand(name = "target", impl = TargetCommand.class),
        @SubCommand(name = "test", impl = TestCommand.class)
        //new SubCommand(name="version",impl=classOf[VersionCommand])
    })
    Command command = null;
}
