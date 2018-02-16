package com.dimajix.flowman.tools.exec.model

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand

class MigrateCommand extends ActionCommand {
    override def executeInternal(executor:Executor, dataflow: Project) : Boolean = {
        false
    }
}
