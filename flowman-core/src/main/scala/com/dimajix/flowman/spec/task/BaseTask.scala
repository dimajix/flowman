package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty


abstract class BaseTask extends Task {
    @JsonProperty(value="description", required=true) private var _description:String = _

}
