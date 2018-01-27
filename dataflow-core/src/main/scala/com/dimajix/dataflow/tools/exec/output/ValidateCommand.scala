package com.dimajix.dataflow.tools.exec.output

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    def executeInternal(executor:Executor, project: Project) : Boolean = {
        false
    }
}
