/*
 * Copyright 2021-2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.hook

import java.io.PrintStream
import java.net.URL
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.io.Resources
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.AssertionToken
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.LifecycleToken
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.metric.CollectingMetricSink
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricWrapper
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionResultWrapper
import com.dimajix.flowman.model.AssertionWrapper
import com.dimajix.flowman.model.BaseHook
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobLifecycle
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobResultWrapper
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.LifecycleResultWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.TargetResultWrapper
import com.dimajix.flowman.model.TargetWrapper
import com.dimajix.flowman.spec.hook.ReportHook.ReporterAssertionToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterJobToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterLifecycleToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterTargetToken
import com.dimajix.flowman.spec.hook.ReportHook.defaultTemplate
import com.dimajix.flowman.spec.metric.MetricBoardSpec
import com.dimajix.flowman.spi.LogFilter


object ReportHook {
    case class ReporterLifecycleToken(output:Option[PrintStream]) extends LifecycleToken
    case class ReporterJobToken(phase:Phase, output:Option[PrintStream], metrics:Option[CollectingMetricSink]) extends JobToken
    case class ReporterTargetToken(phase:Phase, output:Option[PrintStream]) extends TargetToken
    case class ReporterAssertionToken(output:Option[PrintStream]) extends AssertionToken

    val defaultTemplate : URL = Resources.getResource(classOf[ReportHook], "/com/dimajix/flowman/report/text")
}

case class ReportHook(
    instanceProperties: Hook.Properties,
    location:Path,
    mode:OutputMode = OutputMode.OVERWRITE,
    template:URL = defaultTemplate,
    metrics:Option[MetricBoard] = None
) extends BaseHook {
    private val logger = LoggerFactory.getLogger(classOf[ReportHook])

    private def newOutput():Option[PrintStream] = {
        if (location.toString == "stdout") {
            Some(System.out)
        }
        else if (location.toString == "stderr") {
            Some(System.err)
        }
        else {
            val fs = location.getFileSystem(context.hadoopConf)
            val out = mode match {
                case OutputMode.OVERWRITE => fs.create(location)
                case OutputMode.APPEND => fs.append(location)
                case OutputMode.ERROR_IF_EXISTS =>
                    if (fs.exists(location))
                        throw new FileAlreadyExistsException(s"Cannot open report output, file $location already exists")
                    fs.create(location)
                case OutputMode.IGNORE_IF_EXISTS =>
                    if (!fs.exists(location)) {
                        fs.create(location)
                    }
                    else {
                        null
                    }
                case _ => throw new IllegalArgumentException(s"Unsupported output mode $mode")
            }

            Option(out).map(s => new PrintStream(s))
        }
    }

    private def loadResource(name:String) : String = {
        val path = template.getPath
        val url =
            if (path.endsWith("/"))
                new URL(template.toString + name)
            else
                new URL(template.toString + "/" + name)
        Resources.toString(url, Charset.forName("UTF-8"))
    }

    private val assertionStartVtl = loadResource("assertion-start.vtl")
    private val assertionFinishVtl = loadResource("assertion-finish.vtl")
    private val jobStartVtl = loadResource("job-start.vtl")
    private val jobFinishVtl = loadResource("job-finish.vtl")
    private val targetStartVtl = loadResource("target-start.vtl")
    private val targetFinishVtl = loadResource("target-finish.vtl")
    private val lifecycleStartVtl = loadResource("lifecycle-start.vtl")
    private val lifecycleFinishVtl = loadResource("lifecycle-finish.vtl")

    private val logFilters = LogFilter.filters

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startLifecycle(execution:Execution, job:Job, instance:JobLifecycle) : LifecycleToken = {
        logger.info(s"Creating new report to $location")
        val output = newOutput()
        output.foreach { p =>
            val vars = Map(
                "job" -> JobWrapper(job),
                "lifecycle" -> instance.phases.map(_.toString).asJava,
                "status" -> Status.RUNNING.toString
            )
            val text = context.evaluate(lifecycleStartVtl, vars)
            p.print(text)
            p.flush()
        }
        ReporterLifecycleToken(output)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishLifecycle(execution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit = {
        val lifecycleToken = token.asInstanceOf[ReporterLifecycleToken]
        lifecycleToken.output.foreach { p =>
            val vars = Map(
                "job" -> JobWrapper(result.job),
                "lifecycle" -> result.lifecycle.phases.map(_.toString).asJava,
                "status" -> result.status.toString,
                "result" -> LifecycleResultWrapper(result)
            )
            val text = context.evaluate(lifecycleFinishVtl, vars)
            p.print(text)
            p.flush()
            p.close()
            logger.info(s"Closed report at $location")
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startJob(execution:Execution, job: Job, instance: JobDigest, parent:Option[Token]): JobToken = {
        // Add collecting metric sink, only if no metrics board is specified
        val metricSink =
            if (metrics.isEmpty) {
                val sink = new CollectingMetricSink
                execution.metricSystem.addSink(sink)
                Some(sink)
            }
            else {
                None
            }

        // Reset metrics of custom metrics board without adding it
        metrics.foreach { board =>
            board.reset(execution.metricSystem)
        }

        val output = parent.flatMap {
            case ReporterLifecycleToken(output) => output
            case _ => newOutput()
        }
        output.foreach { p =>
            val vars = Map(
                "job" -> JobWrapper(job),
                "phase" -> instance.phase.toString,
                "status" -> Status.RUNNING.toString
            )
            val text = context.evaluate(jobStartVtl, vars)
            p.print(text)
            p.flush()
        }

        ReporterJobToken(instance.phase, output, metricSink)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(execution:Execution, token: JobToken, result: JobResult): Unit = {
        val jobToken = token.asInstanceOf[ReporterJobToken]

        // Remove custom board it
        val boardMetrics = metrics.toSeq.flatMap { board =>
            board.metrics(execution.metricSystem, result.status).map(m => MetricWrapper(m))
        }

        // Grab metrics from Sink and remove it
        val sinkMetrics = jobToken.metrics.toSeq.flatMap { sink =>
            execution.metricSystem.removeSink(sink)
            sink.metrics.map(m => MetricWrapper(m))
        }

        jobToken.output.foreach { p =>
            val vars = Map(
                "job" -> JobWrapper(result.job),
                "phase" -> result.instance.phase.toString,
                "status" -> result.status.toString,
                "result" -> JobResultWrapper(result),
                "metrics" -> (boardMetrics ++ sinkMetrics).sortBy(_.getName()).asJava
            )
            val text = context.evaluate(jobFinishVtl, vars)
            p.print(text)
            p.flush()
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    override def startTarget(execution:Execution, target: Target, instance: TargetDigest, parent: Option[Token]): TargetToken = {
        val output = parent.flatMap {
            case ReporterJobToken(_, output, _) => output
            case _ => None
        }
        output.foreach { p =>
            val vars = Map(
                "target" -> TargetWrapper(target),
                "phase" -> instance.phase.toString,
                "status" -> Status.RUNNING.toString
            )
            val text = context.evaluate(targetStartVtl, vars)
            p.print(text)
            p.flush()
        }
        ReporterTargetToken(instance.phase, output)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(execution:Execution, token: TargetToken, result: TargetResult): Unit = {
        val targetToken = token.asInstanceOf[ReporterTargetToken]
        targetToken.output.foreach { p =>
            val vars = Map(
                "target" -> TargetWrapper(result.target),
                "phase" -> result.instance.phase.toString,
                "status" -> result.status.toString,
                "result" -> TargetResultWrapper(result)
            )
            val text = context.evaluate(targetFinishVtl, vars)
            p.print(text)
            p.flush()
        }
    }

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    override def startAssertion(execution:Execution, assertion: Assertion, parent: Option[Token]): AssertionToken = {
        val output = parent.flatMap {
            case ReporterJobToken(_, output, _) => output
            case ReporterTargetToken(_, output) => output
            case _ => None
        }
        output.foreach { p =>
            val vars = Map(
                "assertion" -> AssertionWrapper(assertion),
                "status" -> Status.RUNNING.toString
            )
            val text = context.evaluate(assertionStartVtl, vars)
            p.print(text)
            p.flush()
        }
        ReporterAssertionToken(output)
    }

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishAssertion(execution:Execution, token: AssertionToken, result: AssertionResult): Unit = {
        val assertionToken = token.asInstanceOf[ReporterAssertionToken]
        assertionToken.output.foreach { p =>
            val vars = Map(
                "assertion" -> AssertionWrapper(result.assertion),
                "status" -> result.status.toString,
                "result" -> AssertionResultWrapper(result)
            )
            val text = context.evaluate(assertionFinishVtl, vars)
            p.print(text)
            p.flush()
        }
    }
}


class ReportHookSpec extends HookSpec {
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="mode", required=false) private var mode:Option[String] = None
    @JsonProperty(value="template", required=false) private var template:String = defaultTemplate.toString
    @JsonProperty(value="metrics", required=false) private var metrics:Option[MetricBoardSpec] = None

    override def instantiate(context: Context, properties:Option[Hook.Properties] = None): ReportHook = {
        ReportHook(
            instanceProperties(context, properties),
            new Path(context.evaluate(location)),
            OutputMode.ofString(context.evaluate(mode).getOrElse("overwrite")),
            new URL(context.evaluate(template)),
            metrics.map(_.instantiate(context))
        )
    }
}
