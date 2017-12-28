package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.dataflow.execution.Context


class AliasMapping extends Mapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _

    def input(implicit context: Context) : String = context.evaluate(_input)

    /**
      * Executes this mapping by returning a DataFrame which corresponds to the specified input
      * @param context
      * @return
      */
    override def execute(implicit context:Context) : DataFrame = {
        context.getTable(input)
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
