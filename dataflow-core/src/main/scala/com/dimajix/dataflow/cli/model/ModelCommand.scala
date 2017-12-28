package com.dimajix.dataflow.cli.model

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.dataflow.cli.Arguments
import com.dimajix.dataflow.cli.Command


class ModelCommand extends Command {
    @Argument(required=false,index=0,metaVar="action",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="create",impl=classOf[CreateCommand]),
        new SubCommand(name="destroy",impl=classOf[DestroyCommand]),
        new SubCommand(name="describe",impl=classOf[DescribeCommand]),
        new SubCommand(name="show",impl=classOf[ShowCommand])
    ))
    var command:Command = _

    override def execute(options:Arguments) : Boolean = {
        super.execute(options)

        if (command == null) {
            new CmdLineParser(this).printUsage(System.err)
            System.err.println
            System.exit(1)
        }

        command.execute(options)
    }
}
