package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context


class EmbeddedSchema extends Schema {
    @JsonProperty(value="fields", required=false) private var _fields: Seq[Field] = _
    @JsonProperty(value="description", required = false) private var _description: String = _

    override def description(implicit context: Context) : String = context.evaluate(_description)
    override def fields(implicit context: Context) : Seq[Field] = _fields
}
