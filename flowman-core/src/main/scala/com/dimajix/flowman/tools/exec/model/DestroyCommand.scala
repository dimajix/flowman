package com.dimajix.flowman.tools.exec.model

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.DestroyRelationTask
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.tools.exec.output.RunCommand


class DestroyCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(usage = "specifies relations to destroy", metaVar = "<relation>")
    var relations: Array[String] = Array()

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Destroying relations {}", if (relations != null) relations.mkString(",") else "all")

        // Then execute output operations
        val toRun =
            if (relations.nonEmpty)
                relations.toSeq
            else
                project.relations.keys.toSeq

        val task = DestroyRelationTask(toRun)
        val job = Job(Seq(task), "Destroy relations")

        val runner = context.runner
        val result = runner.execute(executor, job)

        result
    }
}
