package com.dimajix.dataflow.spec.output

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.TableIdentifier


abstract class BaseOutput extends Output {
    @JsonProperty(value = "enabled", required=false) private var _enabled:String = _
    @JsonProperty(value = "input", required=true) private var _input:String = _
    @JsonProperty(value = "columns", required=false) private var _columns:Map[String,String] = _

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[(String,String)] = if (_columns != null) _columns.mapValues(context.evaluate).toSeq else null

    override def enabled(implicit context:Context) : Boolean = if (_enabled != null) context.evaluate(_enabled).toBoolean else true

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[TableIdentifier] = {
        val table = input
        if (table != null)
            Array(table)
        else
            Array()
    }
}
