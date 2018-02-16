package com.dimajix.flowman.spec.output

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class BlackholeOutput extends BaseOutput {
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : Unit = {
        implicit val context = executor.context
        input(this.input).count()
    }
}
