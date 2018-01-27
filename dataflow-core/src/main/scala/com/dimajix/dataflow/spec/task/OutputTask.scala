package com.dimajix.dataflow.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.execution.OutputRunner
import com.dimajix.dataflow.spec.OutputIdentifier


class OutputTask extends BaseTask {
    @JsonProperty(value="outputs", required=true) private var _outputs:Seq[String] = Seq()

    def outputs(implicit context: Context) : Seq[OutputIdentifier] = _outputs.map(i => OutputIdentifier.parse(context.evaluate(i)))

    def execute(executor:Executor) : Unit = {
        implicit val context = executor.context
        val outs = outputs.map(context.getOutput)
        val runner = new OutputRunner(executor)
        runner.execute(outs)
    }
}
