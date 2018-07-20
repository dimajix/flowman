package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor


class ShowEnvironmentTask extends BaseTask {
    @JsonProperty(value="variables", required=true) private var _variables:Seq[String] = Seq()

    def variables(implicit context: Context) : Seq[String] = _variables.map(context.evaluate)

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val env = executor.context.environment
        if (_variables.nonEmpty) {
            variables.foreach(v => println(v + "=" + env.getOrElse(v, "<undefined>")))
        }
        else {
            env.foreach(kv => println(kv._1 + "=" + kv._2.toString))
        }
        true
    }
}
