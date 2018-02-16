package com.dimajix.flowman.spec.output

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class DumpOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[DumpOutput])

    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit val context = executor.context
        input(this.input).collect().foreach(println)
    }
}
