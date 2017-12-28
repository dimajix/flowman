package com.dimajix.dataflow.cli.model

import com.dimajix.dataflow.cli.Command
import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.Dataflow

class CreateCommand extends AbstractCommand {
    def executeInternal(context:Context, dataflow:Dataflow) : Boolean = {
        false
    }
}
