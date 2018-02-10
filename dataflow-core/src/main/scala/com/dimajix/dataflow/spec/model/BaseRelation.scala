package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.executor
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.schema.Field


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    @JsonProperty(value="external", required=false) private var _external: String = "false"
    @JsonProperty(value="options", required=false) private var _options:Map[String,String] = Map()
    @JsonProperty(value="schema", required=false) private var _schema: Seq[Field] = _
    @JsonProperty(value="defaults", required=false) private var _defaultValues:Map[String,String] = Map()

    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)
    def defaultValues(implicit context: Context) : Map[String,String] = _defaultValues.mapValues(context.evaluate)
    def schema(implicit context: Context) : Seq[Field] = _schema

    protected def reader(executor:Executor) = {
        implicit val context = executor.context
        val reader = executor.spark.read
        options.foreach(kv => reader.option(kv._1, kv._2))
        if (schema != null)
            reader.schema(createSchema)
        reader
    }
    protected def createSchema(implicit context:Context) : StructType = {
        val fields = _schema.map(f => StructField(f.name, f.sparkType))
        StructType(fields)
    }
}
