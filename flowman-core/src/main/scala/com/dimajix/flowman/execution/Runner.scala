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

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.JobRunnerImpl.RunnerJobToken
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.StateStoreAdaptorListener
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.metric.withWallTime
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Result
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestWrapper
import com.dimajix.flowman.spi.LogFilter
import com.dimajix.flowman.util.ConsoleColors._
import com.dimajix.flowman.util.withShutdownHook
import com.dimajix.spark.SparkUtils.withJobGroup


private[execution] sealed class RunnerImpl {
    protected val logger = LoggerFactory.getLogger(classOf[Runner])
    protected val logFilters = LogFilter.filters

    def resultOf(target:Target, phase:Phase, dryRun:Boolean)(fn: => TargetResult) : TargetResult = {
        val result = Try {
            if (!dryRun) {
                fn
            }
            else {
                TargetResult(target, phase, Status.SUCCESS)
            }
        }
        match {
            case Success(r) => r
            case Failure(e) =>
                logger.error(s"Caught exception while executing phase '$phase' for target '${target.identifier}'", e)
                TargetResult(target, phase, e)
        }

        result match {
            case TargetResult(_,_,_,_,Status.SUCCESS,_) =>
                logger.info(green(s"Successfully finished phase '$phase' for target '${target.identifier}'"))
            case TargetResult(_,_,_,_,Status.SKIPPED,_) =>
                logger.info(green(s"Skipped phase '$phase' for target '${target.identifier}'"))
            case TargetResult(_,_,_,_,Status.FAILED,_) =>
                logger.error(red(s"Failed phase '$phase' for target '${target.identifier}'"))
            case TargetResult(_,_,_,_,Status.ABORTED,_) =>
                logger.error(red(s"Aborted phase '$phase' for target '${target.identifier}'"))
            case TargetResult(_,_,_,_,status,_) =>
                logger.warn(yellow(s"Finished '$phase' for target '${target.identifier}' with unknown status $status"))
        }

        result
    }

    def withExecution[T](parent:Execution, isolated:Boolean=false)(fn:Execution => T) : T = {
        val execution : Execution = new ScopedExecution(parent, isolated)
        val result = fn(execution)

        // Wait for any running background operation, and do not perform a cleanup
        val ops = execution.operations
        val activeOps = ops.listActive()
        if (activeOps.nonEmpty) {
            logger.info("Some background operations are still active:")
            activeOps.foreach(o => logger.info(s"  - s${o.name}"))
            logger.info("Waiting for termination...")
            ops.awaitTermination()
        }

        // Finally release any resources
        execution.cleanup()
        result
    }

    private val separator = boldWhite((0 to 79).map(_ => "-").mkString)
    def logSubtitle(s:String) : Unit = {
        val l = (77 - (s.length + 1)) / 2
        val t = if (l > 3) {
            val sep = (0 to l).map(_ => '-').mkString
            boldWhite(sep) + " " + boldCyan(s) + " " + boldWhite(sep)
        }
        else {
            boldWhite("--- ") + boldCyan(s) + boldWhite(" ---")
        }

        logger.info("")
        logger.info(t)
    }

    def logTitle(title:String) : Unit = {
        logger.info("")
        logger.info(separator)
        logger.info(boldWhite(s"  $title"))
        logger.info(separator)
    }

    def logEnvironment(context:Context) : Unit = {
        logger.info("Environment:")
        context.environment.toSeq.sortBy(_._1).foreach { keyValue =>
            logFilters.foldLeft(Option(keyValue))((kv, f) => kv.flatMap(kv => f.filterConfig(kv._1,kv._2.toString)))
                .foreach { case (key,value) => logger.info(s"  $key=$value") }
        }
        logger.info("")
    }

    def logStatus(title:String, status:Status, duration: Duration, endTime:Instant) : Unit = {
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

        logger.info(separator)
        logger.info(msg)
        logger.info(separator)
        logger.info(s"Total time: ${duration.toMillis / 1000.0} s")
        logger.info(s"Finished at: ${endTime.atZone(ZoneId.systemDefault())}")
        logger.info(separator)
    }
}


/**
 * Private implementation of Job specific methods
 */
private[execution] object JobRunnerImpl {
    private final case class RunnerJobToken(tokens:Seq[(RunnerListener, JobToken)]) extends JobToken
    private final case class RunnerTargetToken(tokens:Seq[(RunnerListener, TargetToken)]) extends TargetToken
}
private[execution] final class JobRunnerImpl(runner:Runner) extends RunnerImpl {
    private val stateStore = runner.stateStore
    private val stateStoreListener = new StateStoreAdaptorListener(stateStore)
    private val parentExecution = runner.parentExecution

    /**
     * Executes a single job using the given execution and a map of parameters. The Runner may decide not to
     * execute a specific job, because some information may indicate that the job has already been successfully
     * run in the past. This behaviour can be overridden with the force flag
     * @param phases
     * @return
     */
    def executeJob(job:Job, phases:Seq[Phase], args:Map[String,Any]=Map(), targets:Seq[Regex]=Seq(".*".r), force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false) : Seq[JobResult] = {
        require(args != null)
        require(phases != null)
        require(args != null)

        runner.withJobContext(job, args, force, dryRun) { (jobContext, arguments) =>
            val isolated = job.parameters.nonEmpty || job.environment.nonEmpty
            withExecution(parentExecution, isolated) { execution =>
                Result.flatMap(phases, keepGoing) { phase =>
                    // Check if build phase really contains any active target. Otherwise we skip this phase and mark it
                    // as SUCCESS (an empty list is always executed as SUCCESS)
                    val isActive = job.targets
                        .filter(target => targets.exists(_.unapplySeq(target.name).nonEmpty))
                        .exists { target =>
                            // This might throw exceptions for non-existing targets. The same
                            // exception will be thrown and handeled properly in executeJobPhase
                            try {
                                jobContext.getTarget(target).phases.contains(phase)
                            } catch {
                                case NonFatal(_) => true
                            }
                        }

                    if (isActive)
                        Some(executeJobPhase(execution, jobContext, job, phase, arguments, targets, force, keepGoing, dryRun))
                    else
                        None
                }
            }
        }
    }

    private def executeJobPhase(
        execution: Execution,
        jobContext:Context,
        job:Job, phase:Phase,
        arguments:Map[String,Any],
        targets:Seq[Regex],
        force:Boolean,
        keepGoing:Boolean,
        dryRun:Boolean) : JobResult = {
        runner.withPhaseContext(jobContext, phase) { context =>
            val title = s"${phase.toString.toUpperCase} job '${job.identifier}' ${arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"
            logTitle(title)
            logEnvironment(context)

            val instance = job.instance(arguments.map { case (k, v) => k -> v.toString })
            val allHooks = if (!dryRun) stateStoreListener +: (runner.hooks ++ job.hooks).map(_.instantiate(context)) else Seq()
            val allMetrics = job.metrics.map(_.instantiate(context))

            val startTime = Instant.now()
            val result = withListeners(job, instance, phase, allHooks) { token =>
                withMetrics(execution.metrics, allMetrics) {
                    withWallTime(execution.metrics, job.metadata, phase) {
                        try {
                            val results = executeJobTargets(execution, context, job, phase, targets, token, force, keepGoing, dryRun)
                            JobResult(job, instance, phase, results)
                        }
                        catch {
                            case NonFatal(ex) =>
                                // Primarily exceptions during target instantiation will be caught here
                                logger.error(s"Caught exception during $phase $title:", ex)
                                JobResult(job, instance, phase, ex)
                        }
                    }
                }
            }

            val endTime = Instant.now()
            val duration = Duration.between(startTime, endTime)
            val status = result.status
            logStatus(title, status, duration, endTime)
            result
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
    private def executeTargetPhase(execution: Execution, target:Target, phase:Phase, jobToken:RunnerJobToken, force:Boolean, dryRun:Boolean) : TargetResult = {
        // Create target instance for state server
        val instance = target.instance

        val forceDirty = force || execution.flowmanConf.getConf(FlowmanConf.EXECUTION_TARGET_FORCE_DIRTY)
        val canSkip = !force && checkTarget(instance, phase)

        withListeners(target, instance, phase, jobToken) {
            logSubtitle(s"$phase target '${target.identifier}'")

            // First checkJob if execution is really required
            if (canSkip) {
                logger.info(cyan(s"Target '${target.identifier}' up to date for phase '$phase' according to state store, skipping execution"))
                logger.info("")
                TargetResult(target, phase, Status.SKIPPED)
            }
            else if (!forceDirty && target.dirty(execution, phase) == No) {
                logger.info(cyan(s"Target '${target.identifier}' not dirty in phase $phase, skipping execution"))
                logger.info("")
                TargetResult(target, phase, Status.SKIPPED)
            }
            else {
                resultOf(target, phase, dryRun) {
                    withWallTime(execution.metrics, target.metadata, phase) {
                        target.execute(execution, phase)
                    }
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
    private def executeJobTargets(execution:Execution, context:Context, job:Job, phase:Phase, targets:Seq[Regex], token:RunnerJobToken, force:Boolean, keepGoing:Boolean, dryRun:Boolean) : Seq[TargetResult] = {
        require(phase != null)

        // This will throw an exception if instantiation fails
        val jobTargets = job.targets.map(t => context.getTarget(t))

        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_EXECUTOR_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val executor = ctor.newInstance()

        def targetFilter(target:Target) : Boolean =
            target.phases.contains(phase) && targets.exists(_.unapplySeq(target.name).nonEmpty)

        executor.execute(execution, context, phase, jobTargets, targetFilter, keepGoing) { (execution, target, phase) =>
            val sc = execution.spark.sparkContext
            withJobGroup(sc, target.name, "Flowman target " + target.identifier.toString) {
                executeTargetPhase(execution, target, phase, token, force, dryRun)
            }
        }
    }

    /**
     * Monitors the job execution by invoking all listeners
     * @param job
     * @param phase
     * @param listeners
     * @param fn
     * @return
     */
    private def withListeners(job:Job, instance:JobInstance, phase:Phase, listeners:Seq[RunnerListener])(fn: RunnerJobToken => JobResult) : JobResult = {
        def startJob() : Seq[(RunnerListener, JobToken)] = {
            listeners.flatMap { hook =>
                try {
                    Some((hook, hook.startJob(job, instance, phase)))
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startJob: ${ex.toString}.")
                        None
                }
            }
        }

        def finishJob(tokens:Seq[(RunnerListener, JobToken)], result:JobResult) : Unit = {
            tokens.foreach { case (listener, token)  =>
                try {
                    listener.finishJob(token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishJob: ${ex.toString}.")
                }
            }
        }

        val tokens = startJob()
        withShutdownHook(finishJob(tokens, JobResult(job, instance, phase, Status.FAILED))) {
            val result = fn(RunnerJobToken(tokens))
            finishJob(tokens, result)
            result
        }
    }

    private def withListeners(target:Target, instance:TargetInstance, phase:Phase, job:RunnerJobToken)(fn: => TargetResult) : TargetResult = {
        def startTarget() : Seq[(RunnerListener, TargetToken)] = {
            job.tokens.flatMap { case(listener,jobToken) =>
                try {
                    Some((listener, listener.startTarget(target, instance, phase, Some(jobToken))))
                }
                catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startTarget: ${ex.toString}.")
                        None
                }
            }
        }

        def finishTarget(tokens:Seq[(RunnerListener, TargetToken)], result:TargetResult) : Unit = {
            tokens.foreach { case(listener, token) =>
                try {
                    listener.finishTarget(token, result)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishTarget: ${ex.toString}.")
                }
            }
        }

        val tokens = startTarget()
        withShutdownHook(finishTarget(tokens, TargetResult(target, phase, Status.FAILED))) {
            val result = fn
            finishTarget(tokens, result)
            result
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

    private def withMetrics[T <: Result](metricSystem: MetricSystem, metrics:Option[MetricBoard])(fn: => T) : T = {
        // Publish metrics
        metrics.foreach { metrics =>
            metrics.reset(metricSystem)
            metricSystem.addBoard(metrics)
        }

        // Run original function
        var status:Status = Status.UNKNOWN
        try {
            val result = fn
            status = result.status
            result
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
    }
}


/**
 * Private Implementation for Test specific methods
 * @param runner
 */
private[execution] final class TestRunnerImpl(runner:Runner) extends RunnerImpl {
    private val parentExecution = runner.parentExecution

    def executeTest(test:Test, keepGoing:Boolean=false, dryRun:Boolean=false) : Status = {
        runner.withTestContext(test, dryRun) { context =>
            withExecution(parentExecution, true) { execution =>
                val title = s"Running test '${test.identifier}'"
                logTitle(title)
                logEnvironment(context)

                val instance = test.instance
                val startTime = Instant.now()

                // Get all targets once here. Otherwise the fixtures would be instantiated over and over again for
                // each phase.
                val targets = test.targets.map(t => context.getTarget(t)) ++ test.fixtures.values.map(_.instantiate(context))

                def runPhase(phase: Phase): Seq[TargetResult] = {
                    // Only execute phase if there are targets. This will save some logging outputs
                    if (targets.exists(_.phases.contains(phase))) {
                        runner.withPhaseContext(context, phase) { context =>
                            executeTestTargets(execution, context, targets, phase, keepGoing, dryRun)
                        }
                    }
                    else {
                        Seq()
                    }
                }

                // First create test environment via fixtures
                val buildStatus = Status.ofAll(Lifecycle.BUILD, keepGoing) { phase =>
                    val phaseResults = runPhase(phase)
                    Status.ofAll(phaseResults.map(_.status))
                }
                // Now run tests if fixtures where successful
                val testStatus =
                    if (buildStatus == Status.SUCCESS || keepGoing) {
                        val sc = execution.spark.sparkContext
                        withJobGroup(sc, test.name, "Flowman test " + test.identifier.toString) {
                            executeTestAssertions(execution, context, test, keepGoing, dryRun)
                        }
                    }
                    else {
                        Status.SKIPPED
                    }
                // Finally clean up, even in case of possible failures.
                val destroyStatus = Status.ofAll(Lifecycle.DESTROY, true) { phase =>
                    val phaseResults = runPhase(phase)
                    Status.ofAll(phaseResults.map(_.status))
                }

                // Compute complete status - which is only SUCCESS if all steps have been executed successfully
                val status = if (Seq(buildStatus, testStatus, destroyStatus).forall(_ == Status.SUCCESS))
                    Status.SUCCESS
                else
                    Status.FAILED

                val endTime = Instant.now()
                val duration = Duration.between(startTime, endTime)
                logStatus(title, status, duration, endTime)
                status
            }
        }
    }

    private def executeTestAssertions(
        execution: Execution,
        context:Context,
        test:Test,
        keepGoing:Boolean,
        dryRun:Boolean
    ) : Status = {
        val title = s"assert test '${test.identifier}'"
        logSubtitle(title)

        try {
            val startTime = Instant.now()

            // First instantiate all assertions
            val instances = test.assertions.values.toSeq.map( _.instantiate(context))

            // Execute all assertions
            val runner = new AssertionRunner(context, execution)
            val results = runner.run(instances, keepGoing=keepGoing, dryRun=dryRun)

            val endTime = Instant.now()
            val duration = Duration.between(startTime, endTime)
            val numSucceeded = results.map(_.numSuccesses).sum
            val numFailed = results.map(_.numFailures).sum
            val numExceptions = results.map(_.numExceptions).sum

            logger.info(cyan(s"$numSucceeded assertions passed, $numFailed failed, $numExceptions exceptions"))
            logger.info(cyan(s"Executed ${numSucceeded + numFailed} assertions in  ${duration.toMillis / 1000.0} s"))

            if (numFailed + numExceptions > 0) Status.FAILED else Status.SUCCESS
        }
        catch {
            // Catch all exceptions
            case NonFatal(ex) =>
                logger.error(s"Caught exception during $title:", ex)
                Status.FAILED
        }
    }

    private def executeTestTargets(execution:Execution, context:Context, targets:Seq[Target], phase:Phase, keepGoing:Boolean, dryRun:Boolean) : Seq[TargetResult] = {
        require(phase != null)

        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_EXECUTOR_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val executor = ctor.newInstance()

        def targetFilter(target:Target) : Boolean =
            target.phases.contains(phase)

        executor.execute(execution, context, phase, targets, targetFilter, keepGoing) { (execution, target, phase) =>
            val sc = execution.spark.sparkContext
            withJobGroup(sc, target.name, "Flowman target " + target.identifier.toString) {
                executeTestTargetPhase(execution, target, phase, dryRun)
            }
        }
    }

    private def executeTestTargetPhase(execution: Execution, target:Target, phase:Phase, dryRun:Boolean) : TargetResult = {
        logSubtitle(s"$phase target '${target.identifier}'")

        // First checkJob if execution is really required
        resultOf(target, phase, dryRun) {
            target.execute(execution, phase)
        }
    }
}


/**
 * The [[Runner]] class should be used for executing jobs, targets and tests. It will take care of applying additonal
 * environment variables, measuring execution time, publishing metrics, error handling and more.
 *
 * @param parentExecution
 * @param stateStore
 * @param hooks
 */
final class Runner(
    private[execution] val parentExecution:Execution,
    private[execution] val stateStore: StateStore,
    private[execution] val hooks: Seq[Template[Hook]]=Seq()
) {
    require(parentExecution != null)
    require(stateStore != null)
    require(hooks != null)

    private val logger = LoggerFactory.getLogger(classOf[Runner])

    /**
      * Executes a single job using the given execution and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overridden with the force flag
      * @param phases
      * @return
      */
    def executeJob(job:Job, phases:Seq[Phase], args:Map[String,Any]=Map(), targets:Seq[Regex]=Seq(".*".r), force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false) : Status = {
        require(args != null)
        require(phases != null)
        require(args != null)

        logger.info(s"Executing phases ${phases.map(p => "'" + p + "'").mkString(",")} for job '${job.identifier}'")
        val runner = new JobRunnerImpl(this)
        val result = runner.executeJob(job, phases, args, targets, force, keepGoing, dryRun)
        Status.ofAll(result.map(_.status))
    }

    /**
     * Executes an individual test.
     * @param test
     * @param keepGoing - Continue running assertions even if unexpected exceptions are raised.
     * @param dryRun
     * @return
     */
    def executeTest(test:Test, keepGoing:Boolean=false, dryRun:Boolean=false) : Status = {
        val runner = new TestRunnerImpl(this)
        runner.executeTest(test, keepGoing, dryRun)
    }

    /**
     * Executes a single target using the given execution and a map of parameters. The Runner may decide not to
     * execute a specific target, because some information may indicate that the job has already been successfully
     * run in the past. This behaviour can be overriden with the force flag
     * @param targets
     * @param phases
     * @return
     */
    def executeTargets(targets:Seq[Target], phases:Seq[Phase], force:Boolean, keepGoing:Boolean=false, dryRun:Boolean=false) : Status = {
        if (targets.nonEmpty) {
            val context = targets.head.context
            val job = Job.builder(context)
                .setName("execute-target")
                .setTargets(targets.map(_.identifier))
                .build()

            val runner = new JobRunnerImpl(this)
            val result = runner.executeJob(job, phases, Map(), Seq(".*".r), force, keepGoing, dryRun)
            Status.ofAll(result.map(_.status))
        }
        else {
            Status.SUCCESS
        }
   }

    /**
     * Provides a context for the given job. This will apply all environment variables of the job and add
     * additional variables like a `force` flag.
     * @param job
     * @param args
     * @param force
     * @param fn
     * @tparam T
     * @return
     */
    def withJobContext[T](job:Job, args:Map[String,Any]=Map(), force:Boolean=false, dryRun:Boolean=false)(fn:(Context,Map[String,Any]) => T) : T = {
        val arguments : Map[String,Any] = job.parameters.flatMap(p => p.default.map(d => p.name -> d)).toMap ++ args
        arguments.toSeq.sortBy(_._1).foreach { case (k,v) => logger.info(s"Job argument $k=$v")}

        verifyArguments(job,arguments)

        val rootContext = RootContext.builder(job.context)
            .withEnvironment("force", force)
            .withEnvironment("dryRun", dryRun)
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

    /**
     * Provides a context for a given test. This will apply all environment variables of the test case and add
     * additional variables like a `force` flag.
     * @param test
     * @param dryRun
     * @param fn
     * @tparam T
     * @return
     */
    def withTestContext[T](test:Test, dryRun:Boolean=false)(fn:(Context) => T) : T = {
        val project = test.project.map(_.name)
        val rootContext = RootContext.builder(test.context)
            .withEnvironment("force", false)
            .withEnvironment("dryRun", dryRun)
            .withEnvironment("test", TestWrapper(test))
            .withEnvironment(test.environment, SettingLevel.JOB_OVERRIDE)
            .overrideRelations(test.overrideRelations.map(kv => RelationIdentifier(kv._1, project) -> kv._2))
            .overrideMappings(test.overrideMappings.map(kv => MappingIdentifier(kv._1, project) -> kv._2))
            .build()
        val projectContext = if (test.context.project.nonEmpty)
            rootContext.getProjectContext(test.context.project.get)
        else
            rootContext
        fn(projectContext)
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
    def withEnvironment[T](job:Job, phase:Phase, args:Map[String,Any], force:Boolean, dryRun:Boolean)(fn:Environment => T) : T = {
        withJobContext(job, args, force, dryRun) { (jobContext,_) =>
            withPhaseContext(jobContext, phase) { context =>
                fn(context.environment)
            }
        }
    }

    def withEnvironment[T](test:Test, dryRun:Boolean)(fn:Environment => T) : T = {
        withTestContext(test, dryRun) { context =>
            fn(context.environment)
        }
    }

    private def verifyArguments(job:Job, arguments:Map[String,Any]) : Unit = {
        // Verify job arguments. This is moved from the constructor into this place, such that only this method throws an exception
        val argNames = arguments.keySet
        val paramNames = job.parameters.map(_.name).toSet
        argNames.diff(paramNames).foreach(p => throw new IllegalArgumentException(s"Unexpected argument '$p' not defined in job '${job.identifier}'"))
        paramNames.diff(argNames).foreach(p => throw new IllegalArgumentException(s"Required parameter '$p' not specified for job '${job.identifier}'"))
    }
}
