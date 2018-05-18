package com.dimajix.flowman.spec.task

import com.dimajix.flowman.execution.Executor


class DumpEnvironmentTask extends BaseTask {
    override def execute(executor:Executor) : Boolean = {
        val env = executor.context.environment
        env.foreach(kv => println(kv._1 + "=" + kv._2.toString))
        true
    }
}
