package com.dimajix.dataflow.tools.exec.output

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.execution.OutputRunner
import com.dimajix.dataflow.spec.OutputIdentifier
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.ActionCommand


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
        val result = Try {
            val toRun =
                if (all)
                    project.outputs.keys.toSeq
                else if (outputs.nonEmpty)
                    outputs.toSeq
                else
                    project.outputs.filter(_._2.enabled).keys.toSeq
            val outs = outputs.map(OutputIdentifier.parse).map(context.getOutput)
            val runner = new OutputRunner(executor)
            runner.execute(outs)
            true
        } match {
            case Success(true) =>
                logger.info("Successfully finished execution of DataFlow")
                true
            case Success(false) =>
                logger.error("Execution of DataFlow failed")
                false
            case Failure(e) =>
                logger.error("Caught exception while executing dataflow: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }

        // Cleanup caches, but after printing error message. Otherwise it looks confusing when the error occured
        executor.cleanup()

        result
    }

    private def executeAndMonitor(executor:Executor, project:Project) : Boolean = {
        val context = executor.context
/*
        // Get Monitor
        val monitor = scala.Option(project.monitor).getOrElse(new NullMonitor)
        val skip = !force && monitor.check(context)
        val runToken = monitor.start(context)

        val shutdownHook = new Thread() { override def run() : Unit = monitor.failure(context, runToken) }
        withShutdownHook(shutdownHook) {
            // First check if execution is really required
            if (skip) {
                logger.info("Everything up to date, skipping execution")
                monitor.skip(context, runToken)
                true
            }
            else {
                val result = executeInternal(executor)
                if (result)
                    monitor.success(context, runToken)
                else
                    monitor.failure(context, runToken)
                result
            }
        }*/
        false
    }

    private def withShutdownHook[T](shutdownHook:Thread)(block: => T) : T = {
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        val result = block
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        result
    }
}
