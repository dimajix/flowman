package com.dimajix.dataflow.tools.dfexec.model

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.dataflow.tools.dfexec.Arguments
import com.dimajix.dataflow.tools.dfexec.Command
import com.dimajix.dataflow.tools.dfexec.NestedCommand


class ModelCommand extends NestedCommand {
    @Argument(required=false,index=0,metaVar="action",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="create",impl=classOf[CreateCommand]),
        new SubCommand(name="destroy",impl=classOf[DestroyCommand]),
        new SubCommand(name="describe",impl=classOf[DescribeCommand]),
        new SubCommand(name="show",impl=classOf[ShowCommand])
    ))
    override var command:Command = _

    override def execute(options:Arguments) : Boolean = {
        super.execute(options)

        command.execute(options)
    }
}
