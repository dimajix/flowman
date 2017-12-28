package com.dimajix.dataflow.cli

import org.kohsuke.args4j.CmdLineParser

abstract class NestedCommand extends Command {
    var command:Command

    override def execute(options:Arguments) : Boolean = {
        if (help || command == null) {
            new CmdLineParser(if (command != null) command else this).printUsage(System.err)
            System.err.println
            System.exit(1)
        }

        true
    }
}
