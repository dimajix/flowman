package com.dimajix.dataflow.spec.output

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.util.SchemaUtils


class RelationOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[RelationOutput])

    @JsonProperty(value="target", required=true) private var _target:String = _
    @JsonProperty(value="mode", required=false) private[spec] var _mode:String = "overwrite"
    @JsonProperty(value="partitions", required=false) private[spec] var _partitions:Array[String] = _
    @JsonProperty(value="parallelism", required=false) private[spec] var _parallelism:String = "16"

    def target(implicit context: Context) : String = context.evaluate(_target)
    def mode(implicit context: Context) : String = context.evaluate(_mode)
    def partitions(implicit context: Context) : Array[String] = if (_partitions != null) _partitions.map(context.evaluate) else Array[String]()
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt

    override def execute(implicit context: Context) = {
        logger.info("Writing to relation {} with format {}", target, format.toString)
        val relation = context.getRelation(target)
        val table = context.getTable(input).coalesce(parallelism)
        val fields = columns
        val schema = if (fields != null) SchemaUtils.createSchema(fields) else null
        relation.write(context, table, null, mode)
    }
}
