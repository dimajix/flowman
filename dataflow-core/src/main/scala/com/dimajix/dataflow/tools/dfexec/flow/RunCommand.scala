package com.dimajix.dataflow.tools.dfexec.flow

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.Project

class RunCommand extends AbstractCommand {
    def executeInternal(context:Context, dataflow:Project) : Boolean = {
        false
    }

}
