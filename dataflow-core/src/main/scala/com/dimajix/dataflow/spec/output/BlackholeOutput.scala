package com.dimajix.dataflow.spec.output

import com.dimajix.dataflow.execution.Context


class BlackholeOutput extends BaseOutput {
    override def execute(implicit context: Context) = {
        context.getTable(input).count()
    }
}
