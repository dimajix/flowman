package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.util.SchemaUtils


class OutputMapping extends Mapping {
    @JsonProperty(value = "input", required=true) private var _input:String = _
    @JsonProperty(value = "target", required=true) private var _target:String = _
    @JsonProperty(value = "columns", required=false) private var _columns:Map[String,String] = _

    def input(implicit context:Context) : String = context.evaluate(_input)
    def target(implicit context:Context) : String  = context.evaluate(_target)
    def columns(implicit context: Context) : Seq[(String,String)] = if (_columns != null) _columns.mapValues(context.evaluate).toSeq else null

    override def execute(implicit context: Context): DataFrame = {
        val relation = context.getRelation(target)
        val table = context.getTable(input)
        val fields = columns
        val schema = if (fields != null) SchemaUtils.createSchema(fields) else null
        relation.write(context, table)
        table
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
