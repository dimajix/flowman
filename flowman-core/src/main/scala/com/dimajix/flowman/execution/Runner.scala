/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.Locale

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Runner.RunnerJobToken
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.metric.withWallTime
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.util.withShutdownHook

object Runner {
    private final case class RunnerJobToken(tokens:Seq[(JobListener, JobToken)]) extends JobToken
    private final case class RunnerTargetToken(tokens:Seq[(JobListener, TargetToken)]) extends TargetToken
}


final class Runner(
    parentExecution:Execution,
    stateStore: StateStore,
    hooks: Seq[Template[Hook]]=Seq()
) {
    require(parentExecution != null)
    require(stateStore != null)
    require(hooks != null)

    private val logger = LoggerFactory.getLogger(classOf[Runner])
    private val separator = (0 to 79).map(_ => "-").mkString
    private def centerPad(s:String) : String = {
        val l = (77 - (s.length + 1)) / 2
        if (l > 3) {
            val sep = (0 to l).map(_ => '-').mkString
            sep + " " + s + " " + sep
        }
        else {
            "--- " + s + " ---"
        }
    }

    /**
      * Executes a single job using the given execution and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overridden with the force flag
      * @param phases
      * @return
      */
    def executeJob(job:Job, phases:Seq[Phase], args:Map[String,Any]=Map(), targets:Seq[Regex]=Seq(".*".r), force:Boolean=false, keepGoing:Boolean=false) : Status = {
        require(args != null)
        require(phases != null)
        require(args != null)

        logger.info(s"Executing phases ${phases.map(p => "'" + p + "'").mkString(",")} for job '${job.identifier}'")

        withJobContext(job, args, force) { (jobContext, arguments) =>
            withExecution(job) { execution =>
                Status.ofAll(phases) { phase =>
                    executeJobPhase(execution, jobContext, job, phase, arguments, targets, force, keepGoing)
                }
            }
        }
    }

    /**
     * Executes a single target using the given execution and a map of parameters. The Runner may decide not to
     * execute a specific target, because some information may indicate that the job has already been successfully
     * run in the past. This behaviour can be overriden with the force flag
     * @param targets
     * @param phases
     * @return
     */
    def executeTargets(targets:Seq[Target], phases:Seq[Phase], force:Boolean, keepGoing:Boolean=false) : Status = {
        if (targets.nonEmpty) {
            val context = targets.head.context
            val job = Job.builder(context)
                .setName("execute-target")
                .setTargets(targets.map(_.identifier))
                .build()

            withJobContext(job, force) { context =>
                withExecution(job) { execution =>
                    Status.ofAll(phases) { phase =>
                        executeJobPhase(execution, context, job, phase, Map(), Seq(".*".r), force, keepGoing)
                    }
                }
            }
        }
        else {
            Status.SUCCESS
        }
   }

    /**
     * Provides a context for the given job
     * @param job
     * @param args
     * @param force
     * @param fn
     * @tparam T
     * @return
     */
    def withJobContext[T](job:Job, args:Map[String,Any]=Map(), force:Boolean=false)(fn:(Context,Map[String,Any]) => T) : T = {
        val arguments : Map[String,Any] = job.parameters.flatMap(p => p.default.map(d => p.name -> d)).toMap ++ args
        arguments.toSeq.sortBy(_._1).foreach { case (k,v) => logger.info(s"Job argument $k=$v")}

        verifyArguments(job,arguments)

        val rootContext = RootContext.builder(job.context)
            .withEnvironment("force", force)
            .withEnvironment("job", JobWrapper(job))
            .withEnvironment(arguments, SettingLevel.SCOPE_OVERRIDE)
            .withEnvironment(job.environment, SettingLevel.JOB_OVERRIDE)
            .build()
        val jobContext = if (job.context.project.nonEmpty)
            rootContext.getProjectContext(job.context.project.get)
        else
            rootContext
        fn(jobContext, arguments)
    }

    def withJobContext[T](job:Job, force:Boolean)(fn:Context => T) : T = {
        val context = ScopeContext.builder(job.context)
            .withEnvironment("force", force)
            .withEnvironment("job", JobWrapper(job))
            .build()
        fn(context)
    }

        /**
     * Creates an code environment containing a [[Context]] for the specified phase
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    def withPhaseContext[T](jobContext:Context, phase:Phase)(fn:Context => T) : T = {
        val context = ScopeContext.builder(jobContext)
            .withEnvironment("phase", phase.toString)
            .build()
        fn(context)
    }

    /**
     * Creates an code environment containing a [[Environment]] for the specified phase
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    def withEnvironment[T](job:Job, phase:Phase, args:Map[String,Any]=Map(), force:Boolean=false)(fn:Environment => T) : T = {
        withJobContext(job, args, force) { (jobContext,_) =>
            withPhaseContext(jobContext, phase) { context =>
                fn(context.environment)
            }
        }
    }

    def withExecution[T](job:Job)(fn:Execution => T) : T = {
        val isolated = job.parameters.nonEmpty || job.environment.nonEmpty
        val execution : Execution = if (isolated) new ScopedExecution(parentExecution) else parentExecution
        val result = fn(execution)
        if (isolated) {
            execution.cleanup()
        }
        result
    }

    private def verifyArguments(job:Job, arguments:Map[String,Any]) : Unit = {
        // Verify job arguments. This is moved from the constructor into this place, such that only this method throws an exception
        val argNames = arguments.keySet
        val paramNames = job.parameters.map(_.name).toSet
        argNames.diff(paramNames).foreach(p => throw new IllegalArgumentException(s"Unexpected argument '$p' not defined in job '${job.identifier}'"))
        paramNames.diff(argNames).foreach(p => throw new IllegalArgumentException(s"Required parameter '$p' not specified for job '${job.identifier}'"))
    }

    private def executeJobPhase(execution: Execution, jobContext:Context, job:Job, phase:Phase, arguments:Map[String,Any], targets:Seq[Regex], force:Boolean, keepGoing:Boolean) : Status = {
        withPhaseContext(jobContext, phase) { context =>
            val title = s"$phase job '${job.identifier}' ${arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"
            logger.info("")
            logger.info(separator)
            logger.info(s"  $title")
            logger.info(separator)

            context.environment.toSeq.sortBy(_._1).foreach { case (k, v) => logger.info(s"Environment $k=$v") }
            logger.info("")

            val instance = job.instance(arguments.map { case (k, v) => k -> v.toString })
            val allHooks = (hooks ++ job.hooks).map(_.instantiate(context))
            val allMetrics = job.metrics.map(_.instantiate(context))

            withMetrics(execution.metrics, allMetrics) {
                val startTime = Instant.now()
                val status = recordJob(instance, phase, allHooks) { token =>
                    try {
                        withWallTime(execution.metrics, job.metadata, phase) {
                            executeJobTargets(execution, context, job, phase, targets, token, force, keepGoing)
                        }
                    }
                    catch {
                        case NonFatal(ex) =>
                            logger.error(s"Caught exception during $title:", ex)
                            Status.FAILED
                    }
                }
                val endTime = Instant.now()
                val duration = Duration.between(startTime, endTime)

                val msg = status match {
                    case Status.SUCCESS|Status.SKIPPED|Status.ABORTED|Status.FAILED =>
                        s"${status.toString.toUpperCase(Locale.ROOT)} $title"
                    case Status.RUNNING =>
                        s"ALREADY RUNNING $title"
                    case status =>
                        s"UNKNOWN STATE '$status' in $title. Assuming failure"
                }

                logger.info(separator)
                logger.info(msg)
                logger.info(separator)
                logger.info(s"Total time: ${duration.toMillis / 1000.0} s")
                logger.info(s"Finished at: ${endTime.atZone(ZoneId.systemDefault())}")
                logger.info(separator)
                status
            }
        }
    }

    /**
      * Executes a single target using the given execution and a map of parameters. The Runner may decide not to
      * execute a specific target, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overriden with the force flag
      * @param target
      * @param phase
      * @return
      */
    private def executeTargetPhase(execution: Execution, target:Target, phase:Phase, jobToken:RunnerJobToken, force:Boolean) : Status = {
        // Create target instance for state server
        val instance = target.instance

        val forceDirty = force || execution.flowmanConf.getConf(FlowmanConf.EXECUTION_TARGET_FORCE_DIRTY)
        val canSkip = !force && checkTarget(instance, phase)

        recordTarget(instance, phase, jobToken) {
            logger.info("")
            logger.info(centerPad(s"$phase target '${target.identifier}'"))

            // First checkJob if execution is really required
            if (canSkip) {
                logger.info(s"Target '${target.identifier}' up to date for phase '$phase' according to state store, skipping execution")
                logger.info("")
                Status.SKIPPED
            }
            else if (!forceDirty && target.dirty(execution, phase) == No) {
                logger.info(s"Target '${target.identifier}' not dirty in phase $phase, skipping execution")
                logger.info("")
                Status.SKIPPED
            }
            else {
                Try {
                    withWallTime(execution.metrics, target.metadata, phase) {
                        target.execute(execution, phase)
                    }
                }
                match {
                    case Success(_) =>
                        logger.info(s"Successfully finished phase '$phase' for target '${target.identifier}'")
                        logger.info("")
                        Status.SUCCESS
                    case Failure(NonFatal(e)) =>
                        logger.error(s"Caught exception while executing phase '$phase' for target '${target.identifier}'", e)
                        logger.info("")
                        Status.FAILED
                }
            }
        }
    }

    /**
     * Executes a single phase of the job. This method will also check if the arguments passed to the constructor
     * are correct and sufficient, otherwise an IllegalArgumentException will be thrown.
     *
     * @param context
     * @param phase
     * @param token
     * @return
     */
    private def executeJobTargets(execution:Execution, context:Context, job:Job, phase:Phase, targets:Seq[Regex], token:RunnerJobToken, force:Boolean, keepGoing:Boolean) : Status = {
        require(phase != null)

        val jobTargets = job.targets.map(t => context.getTarget(t))

        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_EXECUTOR_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val executor = ctor.newInstance()

        def targetFilter(target:Target) : Boolean =
            target.phases.contains(phase) && targets.exists(_.unapplySeq(target.name).nonEmpty)

        executor.execute(execution, context, phase, jobTargets, targetFilter, keepGoing) { (execution, target, phase) =>
            executeTargetPhase(execution, target, phase, token, force)
        }
    }

    /**
     * Monitors the job execution by invoking all hooks and the state store
     * @param job
     * @param phase
     * @param hooks
     * @param fn
     * @return
     */
    private def recordJob(job:JobInstance, phase:Phase, hooks:Seq[Hook])(fn: RunnerJobToken => Status) : Status = {
        def startJob() : Seq[(JobListener, JobToken)] = {
            Seq((stateStore, stateStore.startJob(job, phase))) ++
            hooks.flatMap { hook =>
                try {
                    Some((hook, hook.startJob(job, phase)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn("Execution listener threw exception on startJob.", ex)
                        None
                }
            }
        }

        def finishJob(tokens:Seq[(JobListener, JobToken)], status:Status) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishJob(token, status)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn("Execution listener threw exception on finishJob.", ex)
                }
            }
        }

        val tokens = startJob()
        withShutdownHook(finishJob(tokens, Status.FAILED)) {
            val status = fn(RunnerJobToken(tokens))
            finishJob(tokens, status)
            status
        }
    }

    private def recordTarget(target:TargetInstance, phase:Phase, job:RunnerJobToken)(fn: => Status) : Status = {
        def startTarget() : Seq[(JobListener, TargetToken)] = {
            job.tokens.flatMap { case(listener,jobToken) =>
                try {
                    Some((listener, listener.startTarget(target, phase, Some(jobToken))))
                }
                catch {
                    case NonFatal(ex) =>
                        logger.warn("Execution listener threw exception on startTarget.", ex)
                        None
                }
            }
        }

        def finishTarget(tokens:Seq[(JobListener, TargetToken)], status:Status) : Unit = {
            tokens.foreach { case(listener, token) =>
                try {
                    listener.finishTarget(token, status)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn("Execution listener threw exception on finishTarget.", ex)
                }
            }
        }

        val tokens = startTarget()
        withShutdownHook(finishTarget(tokens, Status.FAILED)) {
            val status = fn
            finishTarget(tokens, status)
            status
        }
    }

    /**
      * Performs some checks, if the target is already up to date
      * @param target
      * @return
      */
    private def checkTarget(target:TargetInstance, phase:Phase) : Boolean = {
        def checkState(state:TargetState) : Boolean = {
            val lifecycle = Lifecycle.ofPhase(phase)
            if (!lifecycle.contains(state.phase)) {
                // Different lifecycle => target is not valid
                false
            } else if (lifecycle.indexOf(state.phase) < lifecycle.indexOf(phase)) {
                // Same lifecycle, but previous phase => target is not valid
                false
            } else {
                state.status == Status.SUCCESS || state.status == Status.SKIPPED
            }
        }

        stateStore.getTargetState(target) match {
            case Some(state:TargetState) => checkState(state)
            case _ => false
        }
    }

    private def withMetrics(metricSystem: MetricSystem, metrics:Option[MetricBoard])(fn: => Status) : Status = {
        // Publish metrics
        metrics.foreach { metrics =>
            metrics.reset(metricSystem)
            metricSystem.addBoard(metrics)
        }

        // Run original function
        var status:Status = Status.UNKNOWN
        try {
            status = fn
        }
        catch {
            case NonFatal(ex) =>
                status = Status.FAILED
                throw ex
        }
        finally {
            // Unpublish metrics
            metrics.foreach { metrics =>
                // Do not publish metrics for skipped jobs
                if (status != Status.SKIPPED) {
                    metricSystem.commitBoard(metrics, status)
                }
                metricSystem.removeBoard(metrics)
            }
        }

        status
    }
}
