package com.dimajix.dataflow.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.util.SchemaUtils


class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _

    def name(implicit context: Context) : String = context.evaluate(_name)
    def ftype : FieldType = _type
    def dtype(implicit context: Context) : DataType = _type.dtype
    def nullable(implicit context: Context) : Boolean = context.evaluate(_nullable).toBoolean
    def description(implicit context: Context) : String = context.evaluate(_description)
}
