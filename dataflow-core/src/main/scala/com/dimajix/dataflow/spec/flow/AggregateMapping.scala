package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context


class AggregateMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[AggregateMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "dimensions", required = true) private[spec] var _dimensions:Array[String] = _
    @JsonProperty(value = "aggregations", required = true) private[spec] var _aggregations:Map[String,String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var _partitions:String = _

    def input(implicit context: Context) : String = context.evaluate(_input)
    def dimensions(implicit context: Context) : Seq[String] = _dimensions.map(context.evaluate)
    def aggregations(implicit context: Context) : Map[String,String] = _aggregations.mapValues(context.evaluate)
    def partitions(implicit context: Context) : Int = if (_partitions == null || _partitions.isEmpty) 0 else context.evaluate(_partitions).toInt

    /**
      * Creates an instance of the aggregated table.
      *
      * @param context
      * @return
      */
    override def execute(implicit context:Context): DataFrame = {
        logger.info("Aggregating table {} on dimensions {}", Array(input, dimensions.mkString(",")):_*)

        val df = context.session.table(input)
        val dims = dimensions.map(col)
        val aggs = aggregations.map(kv => expr(kv._2).as(kv._1))
        val parts = partitions
        if (parts > 0)
            df.repartition(parts, dims:_*).groupBy(dims:_*).agg(aggs.head, aggs.tail.toSeq:_*)
        else
            df.groupBy(dims:_*).agg(aggs.head, aggs.tail.toSeq:_*)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[String] = {
        Array(input)
    }
}
