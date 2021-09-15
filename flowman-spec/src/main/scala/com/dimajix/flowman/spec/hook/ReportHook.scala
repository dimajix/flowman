/*
 * Copyright 2021 Kaya Kupferschmidt
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
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.AssertionToken
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.LifecycleToken
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.BaseHook
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.hook.ReportHook.ReporterAssertionToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterJobToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterLifecycleToken
import com.dimajix.flowman.spec.hook.ReportHook.ReporterTargetToken
import com.dimajix.flowman.spi.LogFilter


object ReportHook {
    case class ReporterLifecycleToken(output:Option[PrintStream]) extends LifecycleToken
    case class ReporterJobToken(phase:Phase, output:Option[PrintStream]) extends JobToken
    case class ReporterTargetToken(phase:Phase, output:Option[PrintStream]) extends TargetToken
    case class ReporterAssertionToken(output:Option[PrintStream]) extends AssertionToken
}

case class ReportHook(
    instanceProperties: Hook.Properties,
    location:Path,
    mode:OutputMode = OutputMode.OVERWRITE
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

    private def boldWhite(str:String) : String = str
    private def boldCyan(str:String) : String = str
    private def boldGreen(str:String) : String = str
    private def boldRed(str:String) : String = str
    private def boldYellow(str:String) : String = str

    private val logFilters = LogFilter.filters
    private val smallSeparator = boldWhite((0 to 79).map(_ => "-").mkString)
    private val bigSeparator = boldWhite((0 to 79).map(_ => "=").mkString)

    private def printSubtitle(p:PrintStream, s:String) : Unit = {
        val l = (77 - (s.length + 1)) / 2
        val t = if (l > 3) {
            val sep = (0 to l).map(_ => '-').mkString
            boldWhite(sep) + " " + boldCyan(s) + " " + boldWhite(sep)
        }
        else {
            boldWhite("--- ") + boldCyan(s) + boldWhite(" ---")
        }

        p.println("")
        p.println(t)
    }
    private def printTitle(p:PrintStream, title:String) : Unit = {
        p.println("")
        p.println(smallSeparator)
        p.println(boldWhite(s"  $title"))
        p.println(smallSeparator)
    }
    private def printBigTitle(p:PrintStream, title:String) : Unit = {
        p.println("")
        p.println(bigSeparator)
        p.println(boldWhite(s"  $title"))
        p.println(bigSeparator)
    }
    private def printEnvironment(p:PrintStream, context:Context) : Unit = {
        p.println("Environment:")
        context.environment.toSeq.sortBy(_._1).foreach { keyValue =>
            logFilters.foldLeft(Option(keyValue))((kv, f) => kv.flatMap(kv => f.filterConfig(kv._1,kv._2.toString)))
                .foreach { case (key,value) => p.println(s"  $key=$value") }
        }
    }
    private def printStatus(p:PrintStream, title:String, status:Status, duration: Duration, endTime:Instant) : Unit = {
        val msg = status match {
            case Status.SUCCESS|Status.SKIPPED =>
                boldGreen(s"${status.toString.toUpperCase(Locale.ROOT)} $title")
            case Status.ABORTED|Status.FAILED =>
                boldRed(s"${status.toString.toUpperCase(Locale.ROOT)} $title")
            case Status.RUNNING =>
                boldYellow(s"ALREADY RUNNING $title")
            case status =>
                boldRed(s"UNKNOWN STATE '$status' in $title. Assuming failure")
        }

        p.println(smallSeparator)
        p.println(msg)
        p.println(smallSeparator)
        p.println(s"Total time: ${duration.toMillis / 1000.0} s")
        p.println(s"Finished at: ${endTime.atZone(ZoneId.systemDefault())}")
        p.println(smallSeparator)
    }

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startLifecycle(job:Job, instance:JobInstance, lifecycle:Seq[Phase]) : LifecycleToken = {
        val now = Instant.now()
        logger.info(s"Creating new report to $location")
        val output = newOutput()
        output.foreach { p =>
            printBigTitle(p, s"Processing job ${job.identifier} at $now")
            printEnvironment(p, job.context)
        }
        ReporterLifecycleToken(output)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishLifecycle(token:LifecycleToken, result:LifecycleResult) : Unit = {
        val lifecycleToken = token.asInstanceOf[ReporterLifecycleToken]
        lifecycleToken.output.foreach { p =>
            val endTime = result.endTime
            val duration = result.duration
            val status = result.status
            printStatus(p, s"Finished lifecycle of job ${result.job.identifier}", status, duration, endTime)
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
    override def startJob(job: Job, instance: JobInstance, phase: Phase, parent:Option[Token]): JobToken = {
        val now = Instant.now()
        val output = parent.flatMap {
            case ReporterLifecycleToken(output) => output
            case _ => newOutput()
        }
        output.foreach { p =>
            printTitle(p, s"${phase} job ${job.identifier} at $now")
            if (parent.isEmpty) {
                printEnvironment(p, job.context)
            }
        }
        ReporterJobToken(phase, output)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(token: JobToken, result: JobResult): Unit = {
        val jobToken = token.asInstanceOf[ReporterJobToken]
        jobToken.output.foreach { p =>
            val endTime = result.endTime
            val duration = result.duration
            val status = result.status
            printStatus(p, s"${jobToken.phase} job ${result.job.identifier}", status, duration, endTime)
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    override def startTarget(target: Target, instance: TargetInstance, phase: Phase, parent: Option[Token]): TargetToken = {
        val now = Instant.now()
        val output = parent.flatMap {
            case ReporterJobToken(_, output) => output
            case _ => None
        }
        output.foreach { p =>
            printSubtitle(p, s"${phase} target ${target.identifier} at $now")
        }
        ReporterTargetToken(phase, output)
    }

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(token: TargetToken, result: TargetResult): Unit = {
        val targetToken = token.asInstanceOf[ReporterTargetToken]
        targetToken.output.foreach { p =>
            p.println(s"Finished ${targetToken.phase} target ${result.target.identifier} with status ${result.status} at ${result.endTime}")
        }
    }

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    override def startAssertion(assertion: Assertion, parent: Option[Token]): AssertionToken = {
        val now = Instant.now()
        val output = parent.flatMap {
            case ReporterJobToken(_, output) => output
            case ReporterTargetToken(_, output) => output
            case _ => None
        }
        output.foreach { p =>
            printSubtitle(p, s"Starting EXECUTE assertion ${assertion.name} at $now")
        }
        ReporterAssertionToken(output)
    }

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishAssertion(token: AssertionToken, result: AssertionResult): Unit = {
        val assertionToken = token.asInstanceOf[ReporterAssertionToken]
        assertionToken.output.foreach { p =>
            printSubtitle(p, s"Finished EXECUTE assertion ${result.assertion.name} with status ${result.status} at ${result.endTime}")
        }
    }
}


class ReportHookSpec extends HookSpec {
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="mode", required=false) private var mode:Option[String] = None

    override def instantiate(context: Context): ReportHook = {
        ReportHook(
            instanceProperties(context),
            new Path(context.evaluate(location)),
            OutputMode.ofString(context.evaluate(mode).getOrElse("overwrite"))
        )
    }
}
