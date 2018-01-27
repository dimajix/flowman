package com.dimajix.dataflow.tools.exec

import org.kohsuke.args4j.CmdLineParser

import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project


abstract class NestedCommand extends Command {
    var command:Command

    override def execute(project:Project, session: Session) : Boolean = {
        if (help || command == null) {
            new CmdLineParser(if (command != null) command else this).printUsage(System.err)
            System.err.println
            System.exit(1)
        }

        true
    }
}
