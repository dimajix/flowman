package com.dimajix.dataflow.tools.exec.mapping

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        false
    }
}
