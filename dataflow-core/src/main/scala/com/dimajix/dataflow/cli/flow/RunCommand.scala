package com.dimajix.dataflow.cli.flow

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.Dataflow

class RunCommand extends AbstractCommand {
    def executeInternal(context:Context, dataflow:Dataflow) : Boolean = {
        false
    }

}
