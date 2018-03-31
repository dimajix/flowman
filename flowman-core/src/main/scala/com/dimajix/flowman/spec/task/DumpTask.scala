package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier

class DumpTask extends BaseTask {
    @JsonProperty(value="input", required=true) private var _input:String = _
    @JsonProperty(value="limit", required=true) private[spec] var _limit:String = "100"
    @JsonProperty(value="columns", required=true) private[spec] var _columns:Seq[String] = _

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def limit(implicit context: Context) : Int = context.evaluate(_limit).toInt
    def columns(implicit context: Context) : Seq[String] = if (_columns != null) _columns.map(context.evaluate) else null

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val dfIn = executor.instantiate(this.input)
        val dfOut = if (_columns != null && _columns.nonEmpty)
            dfIn.select(columns.map(c => dfIn(c)):_*)
        else
            dfIn

        dfOut.show(limit)
        true
    }
}
