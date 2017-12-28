package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.dataflow.execution.Context

class RepartitionMapping extends Mapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Seq[String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var _partitions:String = _
    @JsonProperty(value = "sort", required = false) private[spec] var _sort:String = _

    def input(implicit context: Context) : String = context.evaluate(_input)
    def columns(implicit context: Context) :Seq[String] = if (_columns != null) _columns.map(context.evaluate) else Seq[String]()
    def partitions(implicit context: Context) : Int= if (_partitions == null || _partitions.isEmpty) 0 else context.evaluate(_partitions).toInt
    def sort(implicit context: Context) : Boolean = if (_sort == null || _sort.isEmpty) false else context.evaluate(_sort).toBoolean

    override def execute(implicit context:Context) : DataFrame = {
        val df = context.session.table(input)
        val parts = partitions
        val cols = columns.map(col)
        val repartitioned = if (parts > 0) df.repartition(parts, cols:_*) else df.repartition(cols:_*)
        if (sort)
            repartitioned.sortWithinPartitions(cols:_*)
        else
            repartitioned
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
