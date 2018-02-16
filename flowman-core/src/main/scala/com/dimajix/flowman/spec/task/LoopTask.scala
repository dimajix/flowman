package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Executor


class LoopTask extends BaseTask {
    @JsonProperty(value="items", required=true) private var _items:String = _
    @JsonProperty(value="var", required=true) private var _var:String = "item"
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    override def execute(executor:Executor) : Boolean = ???
}
