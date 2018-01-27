package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.util.SchemaUtils


class ProjectMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private var _columns:Seq[Map[String,String]] = Seq()

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) :Seq[(String,String)] = _columns.flatMap(_.mapValues(context.evaluate))

    /**
      * Executes this Mapping and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val df = input(this.input)
        val cols = columns.map(nv => col(nv._1).cast(SchemaUtils.mapType(nv._2)))
        df.select(cols:_*)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[TableIdentifier] = {
        Array(input)
    }
}
