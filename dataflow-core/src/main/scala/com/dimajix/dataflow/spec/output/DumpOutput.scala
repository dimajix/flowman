package com.dimajix.dataflow.spec.output

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context


class DumpOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[DumpOutput])

    override def execute(implicit context: Context) = {
        context.getTable(input).collect().foreach(println)
    }
}
