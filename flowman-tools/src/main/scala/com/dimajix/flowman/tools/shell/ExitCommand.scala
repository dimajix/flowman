package com.dimajix.flowman.tools.shell

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class ExitCommand extends Command {
    override def execute(session: Session, project: Project, context: Context): Boolean = {
        System.exit(0)
        true
    }
}
