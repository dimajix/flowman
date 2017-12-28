package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.dataflow.execution.Context


class FilterMapping extends Mapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "condition", required = true) private var _condition:String = _

    def input(implicit context: Context) = context.evaluate(_input)
    def condition(implicit context: Context) = context.evaluate(_condition)

    override def execute(implicit context:Context) : DataFrame = {
        val df = context.session.table(input)
        df.filter(condition)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[String] = {
        Array(input)
    }
}
