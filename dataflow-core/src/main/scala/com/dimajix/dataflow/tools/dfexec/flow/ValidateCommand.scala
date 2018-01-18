package com.dimajix.dataflow.tools.dfexec.flow

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.ActionCommand


class ValidateCommand extends ActionCommand {
    def executeInternal(context:Context, dataflow:Project) : Boolean = {
        false
    }
}
