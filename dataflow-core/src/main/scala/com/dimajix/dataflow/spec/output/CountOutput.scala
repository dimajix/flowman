package com.dimajix.dataflow.spec.output

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.TableIdentifier


class CountOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[CountOutput])

    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : Unit = {
        implicit val context = executor.context
        val count = input(this.input).count()
        System.out.println(s"Table $input contains $count records")
    }
}
