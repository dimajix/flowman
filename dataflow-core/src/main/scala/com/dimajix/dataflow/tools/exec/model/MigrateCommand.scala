package com.dimajix.dataflow.tools.exec.model

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.ActionCommand

class MigrateCommand extends ActionCommand {
    override def executeInternal(executor:Executor, dataflow: Project) : Boolean = {
        false
    }
}
