package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.util.SchemaUtils


class ProjectMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private var _columns:Seq[Map[String,String]] = Seq()

    def input(implicit context: Context) : String = context.evaluate(_input)
    def columns(implicit context: Context) :Seq[(String,String)] = _columns.flatMap(_.mapValues(context.evaluate))

    override def execute(implicit context:Context) : DataFrame = {
        val df = context.session.table(input)
        val cols = columns.map(nv => col(nv._1).cast(SchemaUtils.mapType(nv._2)))
        df.select(cols:_*)
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
