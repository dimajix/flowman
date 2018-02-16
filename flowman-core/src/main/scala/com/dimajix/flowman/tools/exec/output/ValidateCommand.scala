package com.dimajix.flowman.tools.exec.output

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    def executeInternal(executor:Executor, project: Project) : Boolean = {
        false
    }
}
