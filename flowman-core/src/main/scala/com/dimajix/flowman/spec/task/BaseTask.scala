package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context


abstract class BaseTask extends Task {
    @JsonProperty(value="description", required=true) private var _description:String = _

    def description(implicit context:Context) : String = context.evaluate(_description)
}
