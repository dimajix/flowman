package com.dimajix.flowman.tools.exec.output

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.NestedCommand


class OutputCommand extends NestedCommand {
    @Argument(required=false,index=0,metaVar="task",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="validate",impl=classOf[ValidateCommand]),
        new SubCommand(name="run",impl=classOf[RunCommand])
    ))
    override var command:Command = _

    override def execute(project:Project, session: Session) : Boolean = {
        super.execute(project, session)
        command.execute(project, session)
    }
}
