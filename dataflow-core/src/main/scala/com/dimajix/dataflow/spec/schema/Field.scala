package com.dimajix.dataflow.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField

import com.dimajix.dataflow.execution.Context


class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="granularity", required = false) private var _granularity: String = _

    def name(implicit context: Context) : String = context.evaluate(_name)
    def ftype : FieldType = _type
    def nullable(implicit context: Context) : Boolean = context.evaluate(_nullable).toBoolean
    def description(implicit context: Context) : String = context.evaluate(_description)
    def granularity(implicit context: Context) : String = context.evaluate(_granularity)

    def sparkType(implicit context: Context) : DataType = _type.sparkType
    def sparkField(implicit context: Context) : StructField = StructField(name, sparkType, nullable)

    def interpolate(value: FieldValue)(implicit context: Context) : Iterable[Any] = _type.interpolate(value, granularity)
}
