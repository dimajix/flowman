package com.dimajix.flowman.tools.exec.model

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.CreateRelationTask
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.tools.exec.output.RunCommand


class CreateCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(usage = "specifies relations to create", metaVar = "<relation>")
    var relations: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Creating relations {}", if (relations != null) relations.mkString(",") else "all")

        // Then execute output operations
        val toRun =
            if (relations.nonEmpty)
                relations.toSeq
            else
                project.relations.keys.toSeq

        val task = CreateRelationTask(toRun)
        val job = Job(Seq(task))

        val runner = context.runner
        val result = runner.execute(executor, job)

        result
    }
}
