package com.dimajix.dataflow.spec.param

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context


abstract class BaseParameter extends Parameter {
    @JsonProperty(value = "default", required=true) private var _default:String = _

    override def default(implicit context: Context) : String = context.evaluate(_default)
}
