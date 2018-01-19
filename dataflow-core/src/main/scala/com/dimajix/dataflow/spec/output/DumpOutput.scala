package com.dimajix.dataflow.spec.output

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.TableIdentifier


class DumpOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[DumpOutput])

    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit val context = executor.context
        input(this.input).collect().foreach(println)
    }
}
