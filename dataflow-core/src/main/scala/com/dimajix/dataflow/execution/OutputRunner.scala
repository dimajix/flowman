package com.dimajix.dataflow.execution

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.output.Output


class OutputRunner(executor: Executor) {
    private val logger = LoggerFactory.getLogger(classOf[OutputRunner])

    def execute(output:Output) : Unit = {
        implicit val context = executor.context
        logger.info("Resolving dependencies for output")
        val dependencies = output.dependencies
            .map(d => (d, executor.instantiate(d)))
            .toMap

        logger.info("Executing output")
        output.execute(executor, dependencies)
    }
    def execute(outputs:Seq[Output]) : Unit = {
        outputs.foreach(execute)
    }
}
