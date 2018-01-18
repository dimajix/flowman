package com.dimajix.dataflow.tools.dfexec.model

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.ActionCommand

class MigrateCommand extends ActionCommand {
    def executeInternal(context:Context, dataflow:Project) : Boolean = {
        false
    }

}
