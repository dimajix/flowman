package com.dimajix.dataflow.tools.dfexec.flow

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.dataflow.tools.dfexec.Arguments
import com.dimajix.dataflow.tools.dfexec.Command


class DataflowCommand extends Command {
    @Argument(required=false,index=0,metaVar="action",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="describe",impl=classOf[DescribeCommand]),
        new SubCommand(name="explain",impl=classOf[ExplainCommand]),
        new SubCommand(name="validate",impl=classOf[ValidateCommand]),
        new SubCommand(name="run",impl=classOf[RunCommand]),
        new SubCommand(name="show",impl=classOf[ShowCommand]),
        new SubCommand(name="test",impl=classOf[TestCommand])
    ))
    var command:Command = _

    override def execute(options:Arguments): Boolean = {
        super.execute(options)

        command.execute(options)
    }
}
