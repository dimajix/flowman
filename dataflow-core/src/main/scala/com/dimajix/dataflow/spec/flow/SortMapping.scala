package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.dataflow.execution.Context


class SortMapping extends Mapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private var _columns:Seq[Map[String,String]] = Seq()

    def input(implicit context: Context) : String = context.evaluate(_input)
    def columns(implicit context: Context) :Seq[(String,String)] = _columns.flatMap(_.mapValues(context.evaluate))

    /**
      * Executes this Transform and returns a corresponding DataFrame
      * @param context
      * @return
      */
    override def execute(implicit context:Context) : DataFrame = {
        val df = context.session.table(input)
        val cols = columns.map(nv =>
            if (nv._2.toLowerCase == "desc")
                col(nv._1).desc
            else
                col(nv._1).asc
        )
        df.sort(cols:_*)
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[String] = {
        Array(input)
    }
}
