package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor


case class ShowEnvironmentTask(
    instanceProperties:Task.Properties,
    variables:Seq[String]
) extends BaseTask {
    override def execute(executor:Executor) : Boolean = {
        val env = executor.context.environment
        if (variables.nonEmpty) {
            variables.foreach(v => println(v + "=" + env.getOrElse(v, "<undefined>")))
        }
        else {
            env.toSeq
                .sortBy(_._1)
                .foreach(kv => println(kv._1 + "=" + kv._2.toString))
        }
        true
    }
}



class ShowEnvironmentTaskSpec extends TaskSpec {
    @JsonProperty(value = "variables", required = false) private var variables: Seq[String] = Seq()


    override def instantiate(context: Context): ShowEnvironmentTask = {
        ShowEnvironmentTask(
            instanceProperties(context),
            variables.map(context.evaluate)
        )
    }
}