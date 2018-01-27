package com.dimajix.dataflow.tools.exec.project

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.Command
import com.dimajix.dataflow.tools.exec.NestedCommand


class ProjectCommand extends NestedCommand {
    @Argument(required=false,index=0,metaVar="task",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="run",impl=classOf[RunCommand])
    ))
    override var command:Command = _

    override def execute(project:Project, session: Session) : Boolean = {
        super.execute(project, session)
        command.execute(project, session)
    }
}
