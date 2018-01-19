package com.dimajix.dataflow.spec.param

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context


class DateTimeParameter extends BaseParameter {
    @JsonProperty(value = "min", required=false) private var _min:String = _
    @JsonProperty(value = "max", required=false) private var _max:String = _
    @JsonProperty(value = "period", required=false) private var _period:String = "P1D"
    @JsonProperty(value = "format", required=false) private var _format:String = _

    def values(argument:String)(implicit context:Context) : Iterable[String] = {
        val value = context.evaluate(argument)
    }
}
