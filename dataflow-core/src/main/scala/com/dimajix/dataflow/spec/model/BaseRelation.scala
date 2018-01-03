package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    @JsonProperty(value="database") private var _database: String = _
    @JsonProperty(value="namespace") private var _namespace: String = _
    @JsonProperty(value="entity") private var _entity: String = _
    @JsonProperty(value="external") private var _external: String = _
    @JsonProperty(value="options") private var _options:Map[String,String] = Map()
    @JsonProperty(value="schema") private var _schema: Seq[Field] = Seq()
    @JsonProperty(value="defaults", required=false) private var _defaultValues:Map[String,String] = Map()

    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def namespace(implicit context:Context) : String = context.evaluate(_namespace)
    def entity(implicit context:Context) : String = context.evaluate(_entity)
    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)
    def database(implicit context: Context) : String = context.evaluate(_database)
    def defaultValues(implicit context: Context) : Map[String,String] = _defaultValues.mapValues(context.evaluate)
}
