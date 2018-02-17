package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.util.SchemaUtils


class RelationOutput extends BaseOutput {
    private val logger = LoggerFactory.getLogger(classOf[RelationOutput])

    @JsonProperty(value="target", required=true) private var _target:String = _
    @JsonProperty(value="mode", required=false) private[spec] var _mode:String = "overwrite"
    @JsonProperty(value="partitions", required=false) private[spec] var _partitions:Array[String] = _
    @JsonProperty(value="parallelism", required=false) private[spec] var _parallelism:String = "16"

    def target(implicit context: Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_target))
    def mode(implicit context: Context) : String = context.evaluate(_mode)
    def partitions(implicit context: Context) : Array[String] = if (_partitions != null) _partitions.map(context.evaluate) else Array[String]()
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt

    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit var context = executor.context
        logger.info("Writing to relation {}", target)
        val relation = context.getRelation(target)
        val table = input(this.input).coalesce(parallelism)
        relation.write(executor, table, null, mode)
    }
}
