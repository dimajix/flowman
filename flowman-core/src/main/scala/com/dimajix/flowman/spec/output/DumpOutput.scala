package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class DumpOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[DumpOutput])

    @JsonProperty(value="limit", required=true) private[spec] var _limit:String = "100"

    def limit(implicit context: Context) : Int = context.evaluate(_limit).toInt


    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit val context = executor.context
        input(this.input).limit(limit).collect().foreach(println)
    }
}
