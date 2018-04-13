package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class DeduplicateMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[DeduplicateMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Array[String] = Array()

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)

    /**
      * Creates an instance of the deduplication table.
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]): DataFrame = {
        implicit val context = executor.context
        logger.info("Deduplicating table {} on columns {}", Array(input, columns.mkString(",")):_*)

        val df = input(this.input)
        val cols = if (columns.nonEmpty) columns else df.columns.toSeq
        df.dropDuplicates(cols)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[TableIdentifier] = {
        Array(input)
    }
}
