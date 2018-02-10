package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.ArrayValue
import com.dimajix.dataflow.spec.RangeValue
import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.SingleValue
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.spec.ValueOrRange
import com.dimajix.dataflow.util.SchemaUtils


class InputMapping extends BaseMapping {
    @JsonProperty(value = "source", required = true) private var _source:String = _
    @JsonProperty(value = "columns", required=false) private var _columns:Map[String,String] = _
    @JsonProperty(value = "arguments", required=false) private var _arguments:Map[String,ValueOrRange] = _

    def source(implicit context:Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_source))
    def columns(implicit context:Context) : Map[String,String] = if (_columns != null) _columns.mapValues(context.evaluate) else null
    def arguments(implicit context:Context) : Map[String,ValueOrRange] = {
        _arguments.mapValues(_ match {
            case v:SingleValue => SingleValue(context.evaluate(v.value))
            case v:ArrayValue => ArrayValue(v.value.map(context.evaluate))
            case v:RangeValue => RangeValue(context.evaluate(v.start), context.evaluate(v.end))
        })
    }

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]): DataFrame = {
        implicit val context = executor.context
        val relation = context.getRelation(source)
        val fields = columns
        val schema = if (fields != null) SchemaUtils.createSchema(fields.toSeq) else null
        relation.read(executor, schema)
    }

    /**
      * Returns the dependencies of this mapping, which are empty for an InputMapping
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[TableIdentifier] = {
        Array()
    }
}
