package com.dimajix.dataflow.spec.output

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context


class CountOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[CountOutput])

    override def execute(implicit context: Context) = {
        val count = context.getTable(input).count()
        logger.info("Table {} contains {} records", input:Any, count:Any)
    }
}
