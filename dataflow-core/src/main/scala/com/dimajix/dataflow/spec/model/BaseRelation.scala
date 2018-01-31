package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.ConnectionIdentifier


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    @JsonProperty(value="external") private var _external: String = _
    @JsonProperty(value="options") private var _options:Map[String,String] = Map()
    @JsonProperty(value="schema") private var _schema: Seq[Field] = Seq()
    @JsonProperty(value="defaults", required=false) private var _defaultValues:Map[String,String] = Map()

    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)
    def defaultValues(implicit context: Context) : Map[String,String] = _defaultValues.mapValues(context.evaluate)
}
