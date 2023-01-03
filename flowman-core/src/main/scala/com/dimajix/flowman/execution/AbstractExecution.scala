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

package com.dimajix.flowman.execution

import java.time.Instant

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.withWallTime
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.DocumenterResult
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobLifecycle
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.util.withShutdownHook


abstract class AbstractExecution extends Execution {
    private val logger = LoggerFactory.getLogger(classOf[MonitorExecution])

    /**
     * Invokes a function with a new [[Executor]] that with additional [[ExecutionListener]].
     * @param listeners
     * @param fn
     * @tparam T
     * @return
     */
    override def withListeners[T](listeners:Seq[ExecutionListener])(fn:Execution => T) : T = {
        val execution = new MonitorExecution(delegate, this.listeners ++ listeners.map(l => (l,None)), metricBoard)
        fn(execution)
    }

    /**
     * Invokes a function with a new Executor with a specific [[MetricBoard]]
     * @param metrics
     * @param fn
     * @tparam T
     * @return
     */
    override def withMetrics[T](metrics: Option[MetricBoard])(fn: Execution => T): T = {
        val execution = new MonitorExecution(delegate, listeners, metrics)
        fn(execution)
    }

    /**
     * Monitors the execution of a lifecycle by calling appropriate listeners at the start and end.
     *
     * @param job
     * @param arguments
     * @param phases
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorLifecycle(job: Job, arguments: Map[String, Any], lifecycle: Seq[Phase])(fn: Execution => LifecycleResult): LifecycleResult = {
        def start(instance:JobLifecycle) : Seq[(ExecutionListener, LifecycleToken)] = {
            listeners.flatMap { case(listener,parent) =>
                try {
                    Some((listener, listener.startLifecycle(this, job, instance)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startLifecycle: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens:Seq[(ExecutionListener, LifecycleToken)], result:LifecycleResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishLifecycle(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishLifecycle: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()
        val instance = job.lifecycle(lifecycle, arguments.map { case (k, v) => k -> v.toString })
        def failure() : LifecycleResult = {
            LifecycleResult(job, instance, Status.FAILED, startTime)
        }

        val tokens = start(instance)
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    fn(execution)
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * Monitors the execution of a job by calling appropriate listeners at the start and end.
     *
     * @param job
     * @param arguments
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorJob(job: Job, arguments: Map[String, Any], phase: Phase)(fn: Execution => JobResult): JobResult = {
        def start(instance:JobDigest) : Seq[(ExecutionListener, JobToken)] = {
            listeners.flatMap { case(listener,parent) =>
                try {
                    Some((listener, listener.startJob(this, job, instance, parent)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startJob: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens:Seq[(ExecutionListener, JobToken)], result:JobResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishJob(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishJob: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()
        val instance = job.digest(phase, arguments.map { case (k, v) => k -> v.toString })
        def failure() : JobResult = {
            JobResult(job, instance, Status.FAILED, startTime)
        }

        // Reset all metrics
        metricSystem.resetMetrics()

        // Note that some hooks might reset all metrics (for example the StateStoreAdaptorListener)
        val tokens = start(instance)
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    // The following line will also reset all metrics belonging to the metricBoard
                    com.dimajix.flowman.metric.withMetrics(execution.metricSystem, metricBoard) {
                        withWallTime(execution.metricSystem, job.metadata, phase) {
                            fn(execution)
                        }
                    }
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * Monitors the execution of a target by calling appropriate listeners at the start and end.
     *
     * @param target
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorTarget(target: Target, phase: Phase)(fn: Execution => TargetResult): TargetResult = {
        def start() : Seq[(ExecutionListener, TargetToken)] = {
            listeners.flatMap { case(listener,parent) =>
                try {
                    Some((listener, listener.startTarget(this, target, target.digest(phase), parent)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startTarget: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens:Seq[(ExecutionListener, TargetToken)], result:TargetResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishTarget(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishTarget: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()
        def failure() : TargetResult = {
            TargetResult(target, phase, Status.FAILED, startTime)
        }

        val tokens = start()
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    withWallTime(execution.metricSystem, target.metadata, phase) {
                        fn(execution)
                    }
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * Monitors the execution of an assertion by calling appropriate listeners at the start and end.
     *
     * @param assertion
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorAssertion(assertion: Assertion)(fn: Execution => AssertionResult): AssertionResult = {
        def start() : Seq[(ExecutionListener, AssertionToken)] = {
            listeners.flatMap { case(listener,parent) =>
                try {
                    Some((listener, listener.startAssertion(this, assertion, parent)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startAssertion: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens:Seq[(ExecutionListener, AssertionToken)], result:AssertionResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishAssertion(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishAssertion: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()
        def failure() : AssertionResult = {
            // TODO: On shutdown, the assertion should be in FAILED state
            AssertionResult(assertion, Seq.empty, startTime)
        }

        val tokens = start()
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    fn(execution)
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * Monitors the execution of an measure by calling appropriate listeners at the start and end.
     *
     * @param measure
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorMeasure(measure: Measure)(fn: Execution => MeasureResult): MeasureResult = {
        def start() : Seq[(ExecutionListener, MeasureToken)] = {
            listeners.flatMap { case(listener,parent) =>
                try {
                    Some((listener, listener.startMeasure(this, measure, parent)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startMeasure: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens:Seq[(ExecutionListener, MeasureToken)], result:MeasureResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishMeasure(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishMeasure: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()
        def failure() : MeasureResult = {
            // TODO: On shutdown, the measure should be in FAILED state
            MeasureResult(measure, Seq.empty, startTime)
        }

        val tokens = start()
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    fn(execution)
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * Monitors the execution of an documenter by calling appropriate listeners at the start and end.
     *
     * @param documenter
     * @param fn
     * @tparam T
     * @return
     */
    override def monitorDocumenter(documenter: Documenter)(fn: Execution => DocumenterResult): DocumenterResult = {
        def start(): Seq[(ExecutionListener, DocumenterToken)] = {
            listeners.flatMap { case (listener, parent) =>
                try {
                    Some((listener, listener.startDocumenter(this, documenter, parent)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startDocumenter: ${ex.toString}.")
                        None
                }
            }
        }

        def finish(tokens: Seq[(ExecutionListener, DocumenterToken)], result: DocumenterResult): Unit = {
            tokens.foreach { case (listener, token) =>
                try {
                    listener.finishDocumenter(this, token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishDocumenter: ${ex.toString}.")
                }
            }
        }

        val startTime = Instant.now()

        def failure(): DocumenterResult = {
            // TODO: On shutdown, the documenter should be in FAILED state
            DocumenterResult(documenter, startTime)
        }

        val tokens = start()
        withShutdownHook(finish(tokens, failure())) {
            withTokens(tokens) { execution =>
                val result = try {
                    fn(execution)
                } catch {
                    case NonFatal(ex) =>
                        finish(tokens, failure())
                        throw ex
                }
                finish(tokens, result)
                result
            }
        }
    }

    /**
     * The delegate is responsible for doing the actual work in the context of monitoring.
     * @return
     */
    protected def delegate : Execution = this

    private def withTokens[T <: Token,R](tokens:Seq[(ExecutionListener,T)])(fn:Execution => R) : R = {
        val execution = new MonitorExecution(delegate, tokens.map(lt => (lt._1, Option(lt._2))), metricBoard)
        fn(execution)
    }
}
