package com.dimajix.dataflow.tools.exec

import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project


abstract class Command {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help")
    var help: Boolean = false

    def execute(project:Project, session: Session) : Boolean = {
        if (help) {
            new CmdLineParser(this).printUsage(System.err)
            System.err.println
            System.exit(1)
        }

        true
    }
}
