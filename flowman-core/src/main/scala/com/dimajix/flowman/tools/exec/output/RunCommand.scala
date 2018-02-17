package com.dimajix.flowman.tools.exec.output

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.OutputTask
import com.dimajix.flowman.tools.exec.ActionCommand


class RunCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(usage = "specifies outputs to process", metaVar = "<output>")
    var outputs: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "runs all outputs, even the disabled ones")
    var all: Boolean = false
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Processing outputs {}", if (outputs != null) outputs.mkString(",") else "all")

        // Then execute output operations
        val toRun =
            if (all)
                project.outputs.keys.toSeq
            else if (outputs.nonEmpty)
                outputs.toSeq
            else
                project.outputs.filter(_._2.enabled).keys.toSeq

        val task = new OutputTask
        task.outputs = toRun
        val job = new Job
        job.tasks = Seq(task)

        val runner = context.runner
        val result = runner.execute(executor, job)

        result
    }
}
