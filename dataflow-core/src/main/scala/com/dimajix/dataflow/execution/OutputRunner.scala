package com.dimajix.dataflow.execution

import com.dimajix.dataflow.spec.output.Output


class OutputRunner(executor: Executor) {
    def execute(output:Output) : Unit = {
        implicit val context = executor.context
        val dependencies = output.dependencies
            .map(d => (d, executor.instantiate(d)))
            .toMap
        output.execute(executor, dependencies)
    }
    def execute(outputs:Seq[Output]) : Unit = {
        outputs.foreach(execute)
    }
}
