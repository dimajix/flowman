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
    @JsonProperty(value="header", required=true) private[spec] var _header:String = "true"
    @JsonProperty(value="columns", required=true) private[spec] var _columns:Seq[String] = _

    def limit(implicit context: Context) : Int = context.evaluate(_limit).toInt
    def header(implicit context: Context) : Boolean = context.evaluate(_header).toBoolean
    def columns(implicit context: Context) : Seq[String] = if (_columns != null) _columns.map(context.evaluate) else null


    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : Unit = {
        implicit val context = executor.context
        val dfIn = input(this.input)
        val dfOut = if (_columns != null && _columns.nonEmpty)
            dfIn.select(columns.map(c => dfIn(c)):_*)
        else
            dfIn

        val result = dfOut.limit(limit).collect()
        if (header) {
            println(dfOut.columns.mkString(","))
        }
        result.foreach(record => println(record.mkString(",")))
    }
}
