package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.TableIdentifier


abstract class BaseOutput extends Output {
    @JsonProperty(value = "enabled", required=false) private var _enabled:String = _
    @JsonProperty(value = "input", required=true) private var _input:String = _

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))

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
