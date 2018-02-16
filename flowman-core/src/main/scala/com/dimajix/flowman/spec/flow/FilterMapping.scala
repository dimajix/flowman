package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class FilterMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "condition", required = true) private var _condition:String = _

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def condition(implicit context: Context) : String = context.evaluate(_condition)

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[TableIdentifier] = {
        Array(input)
    }

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val df = input(this.input)
        df.filter(condition)
    }
}
