/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils
import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.text.TimeFormatter
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.AbstractContext.Builder
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.StateStoreAdaptorListener
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Result
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestWrapper
import com.dimajix.flowman.spi.LogFilter
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.util.ConsoleColors._
import com.dimajix.spark.SparkUtils.withJobGroup


private[execution] sealed class RunnerImpl {
    protected val logger = LoggerFactory.getLogger(classOf[Runner])
    protected val logFilters = LogFilter.filters

    def resultOf(target:Target, phase:Phase, dryRun:Boolean)(fn: => TargetResult) : TargetResult = {
        val startTime = Instant.now()
        // Normally, targets should not throw any exception, but instead wrap any error inside the TargetResult.
        // But since we never know, we add a try/catch block
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
            case Failure(e) => TargetResult(target, phase, e, startTime)
        }

        val duration = result.duration
        result.status match {
            case Status.SUCCESS =>
                logger.info(green(s"Successfully finished phase '$phase' for target '${target.identifier}' in ${TimeFormatter.toString(duration)}"))
            case Status.SUCCESS_WITH_ERRORS =>
                logger.warn(yellow(s"Successfully finished phase '$phase' for target '${target.identifier}' with errors  in ${TimeFormatter.toString(duration)}"))
            case Status.SKIPPED =>
                logger.info(green(s"Skipped phase '$phase' for target '${target.identifier}'"))
            case Status.FAILED if result.exception.nonEmpty =>
                logger.error(red(s"Failed phase '$phase' for target '${target.identifier}'  after ${TimeFormatter.toString(duration)} with exception: "), result.exception.get)
            case Status.FAILED =>
                logger.error(red(s"Failed phase '$phase' for target '${target.identifier}' after ${TimeFormatter.toString(duration)}"))
            case Status.ABORTED =>
                logger.error(red(s"Aborted phase '$phase' for target '${target.identifier}' after ${TimeFormatter.toString(duration)}"))
            case status =>
                logger.warn(yellow(s"Finished '$phase' for target '${target.identifier}' with unknown status ${status.upper}"))
        }

        result
    }

    protected val lineSize = 109
    protected val separator = boldWhite(StringUtils.repeat('-', lineSize))
    protected val doubleSeparator = boldWhite(StringUtils.repeat('=', lineSize))
    def logSubtitle(s:String) : Unit = {
        val l = (lineSize - 2 - s.length) / 2
        val t = if (l > 3) {
            val lsep = StringUtils.repeat('-', l)
            val rsep = StringUtils.repeat('-', lineSize - 2 - s.length - l)
            boldWhite(lsep) + " " + boldCyan(s) + " " + boldWhite(rsep)
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
        context.environment.toSeq.sortBy(_._1).foreach { case (key,value) =>
            LogFilter.filter(logFilters, key, value.toString)
                .foreach { case (key,value) => logger.info(s"  $key=$value") }
        }
        logger.info("")
    }

    def logStatus(title:String, status:Status, duration: Duration, endTime:Instant, double:Boolean=false) : Unit = {
        val msg = status match {
            case Status.SUCCESS|Status.SKIPPED =>
                boldGreen(s"${status.upper} $title")
            case Status.SUCCESS_WITH_ERRORS =>
                boldYellow(s"${status.upper} $title")
            case Status.ABORTED|Status.FAILED =>
                boldRed(s"${status.upper} $title")
            case Status.RUNNING =>
                boldYellow(s"ALREADY RUNNING $title")
            case status =>
                boldRed(s"UNKNOWN STATE '$status' in $title. Assuming failure")
        }

        val sep = if (double) doubleSeparator else separator
        logger.info(sep)
        logger.info(msg)
        logger.info(sep)
        logger.info(s"Total time: ${TimeFormatter.toString(duration)}")
        logger.info(s"Finished at: ${endTime.atZone(ZoneId.systemDefault())}")
        logger.info(sep)
    }

    def logJobResult(title:String, result:JobResult) : Unit = {
        if (result.children.length > 1) {
            val args = result.arguments
            logger.info(separator)
            logger.info(boldWhite(s"Execution summary for ${result.category.lower} '${result.identifier}' ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"))
            logger.info("")
            for (child <- result.children) {
                val name = child.identifier.toString
                val status = s"${this.status(child.status)} [${StringUtils.leftPad(TimeFormatter.toString(child.duration), 10)}]"
                val dots = StringUtils.repeat('.', lineSize - child.status.upper.length - name.length - 15)
                logger.info(s"$name $dots $status")
            }
        }
        logStatus(title, result.status, result.duration, result.endTime)
    }

    def logLifecycleResult(title:String, result:LifecycleResult) : Unit = {
        logger.info("")
        if (result.children.length > 1) {
            val args = result.arguments
            logger.info(doubleSeparator)
            logger.info(boldWhite(s"Overall lifecycle summary for ${result.category.lower} '${result.identifier}' ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"))
            logger.info("")
            for (child <- result.children) {
                val name = s"Phase ${child.phase.upper}"
                val status = s"${this.status(child.status)} [${StringUtils.leftPad(TimeFormatter.toString(child.duration), 10)}]"
                val dots = StringUtils.repeat('.', lineSize - child.status.upper.length - name.length - 15)
                logger.info(s"$name $dots $status")
            }
        }
        logStatus(title, result.status, result.duration, result.endTime, double=true)
    }

    private def status(status:Status) : String = {
        status match {
            case Status.SUCCESS|Status.SKIPPED => boldGreen(status.upper)
            case Status.SUCCESS_WITH_ERRORS|Status.RUNNING => boldYellow(status.upper)
            case Status.FAILED|Status.ABORTED => boldRed(status.upper)
            case _ => boldRed(status.upper)
        }
    }
}


/**
 * Private implementation of Job specific methods
 */
private[execution] final class JobRunnerImpl(runner:Runner) extends RunnerImpl {
    private val stateStore = runner.stateStore
    private val stateStoreListener = new StateStoreAdaptorListener(stateStore)

    /**
     * Executes a single job using the given execution and a map of parameters. The Runner may decide not to
     * execute a specific job, because some information may indicate that the job has already been successfully
     * run in the past. This behaviour can be overridden with the force flag
     * @param phases
     * @return
     */
    def executeJob(job:Job, phases:Seq[Phase], args:Map[String,Any]=Map(), targets:Seq[Regex]=Seq(".*".r), dirtyTargets:Seq[Regex]=Seq(), force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false, isolated:Boolean=true) : LifecycleResult = {
        require(args != null)
        require(phases != null)
        require(args != null)

        val prj = job.project.map(prj => s" in project '${prj.name}'${prj.version.map(v => s" version $v").getOrElse("")}").getOrElse("")
        logger.info("")
        logger.info(separator)
        logger.info(s"Executing phases ${phases.map(p => "'" + p + "'").mkString(",")} for job '${job.name}'$prj")

        val startTime = Instant.now()
        val isolated2 = isolated || job.parameters.nonEmpty || job.environment.nonEmpty
        runner.withExecution(isolated2) { execution =>
            runner.withJobContext(job, args, Some(execution), force, dryRun, isolated2) { (context, arguments) =>
                val title = s"lifecycle for job '${job.identifier}' ${arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"
                val listeners = if (!dryRun) stateStoreListener +: (runner.hooks ++ job.hooks).map(_.instantiate(context)) else Seq()
                val result = execution.withListeners(listeners) { execution =>
                    execution.monitorLifecycle(job, arguments, phases) { execution =>
                        val results = Result.flatMap(phases, keepGoing) { phase =>
                            // Check if build phase really contains any active target. Otherwise we skip this phase and mark it
                            // as SUCCESS (an empty list is always executed as SUCCESS)
                            val isActive = job.targets
                                .filter(target => targets.exists(_.unapplySeq(target.name).nonEmpty))
                                .exists { target =>
                                    // This might throw exceptions for non-existing targets. The same
                                    // exception will be thrown and handeled properly in executeJobPhase
                                    try {
                                        context.getTarget(target).phases.contains(phase)
                                    } catch {
                                        case NonFatal(_) => true
                                    }
                                }

                            if (isActive)
                                Some(executeJobPhase(execution, context, job, phase, arguments, targets, dirtyTargets, force=force, keepGoing=keepGoing, dryRun=dryRun))
                            else
                                None
                        }

                        val instance = job.lifecycle(phases, arguments.map { case (k, v) => k -> v.toString })
                        LifecycleResult(job, instance, results, startTime)
                    }
                }

                logLifecycleResult(title, result)
                result
            }
        }
    }

    private def executeJobPhase(
        execution: Execution,
        jobContext:Context,
        job:Job, phase:Phase,
        arguments:Map[String,Any],
        targets:Seq[Regex],
        dirtyTargets:Seq[Regex],
        force:Boolean,
        keepGoing:Boolean,
        dryRun:Boolean) : JobResult = {
        runner.withPhaseContext(jobContext, phase) { context =>
            val title = s"${phase.upper} job '${job.identifier}' ${arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")}"
            logTitle(title)
            logEnvironment(context)

            val allMetrics = job.metrics.map(_.instantiate(context))

            val startTime = Instant.now()
            val result =
                execution.withMetrics(allMetrics) { execution =>
                    execution.monitorJob(job, arguments, phase) { execution =>
                        val instance = job.digest(phase, arguments.map { case (k, v) => k -> v.toString })
                        try {
                            val results = executeJobTargets(execution, context, job, phase, targets, dirtyTargets, force=force, keepGoing=keepGoing, dryRun=dryRun)
                            JobResult(job, instance, results, startTime)
                        }
                        catch {
                            case NonFatal(ex) =>
                                // Primarily exceptions during target instantiation will be caught here
                                logger.error(s"Caught exception during $title: ${reasons(ex)}")
                                JobResult(job, instance, ex, startTime)
                        }
                    }
                }

            logJobResult(title, result)
            result
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
    private def executeJobTargets(execution:Execution, context:Context, job:Job, phase:Phase, targets:Seq[Regex], dirtyTargets:Seq[Regex], force:Boolean, keepGoing:Boolean, dryRun:Boolean) : Seq[TargetResult] = {
        require(phase != null)

        // This will throw an exception if instantiation fails
        val jobTargets = job.targets.map(t => context.getTarget(t))

        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_EXECUTOR_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val executor = ctor.newInstance()

        def targetFilter(target:Target) : Boolean =
            target.phases.contains(phase) && targets.exists(_.unapplySeq(target.name).nonEmpty)

        val dirtyManager = new DirtyTargets(jobTargets, phase)
        dirtyManager.taint(dirtyTargets)

        executor.execute(execution, context, phase, jobTargets, targetFilter, keepGoing) { (execution, target, phase) =>
            val sc = execution.spark.sparkContext
            withJobGroup(sc, target.name, s"$phase target ${target.identifier}") {
                val dirty = dirtyManager.isDirty(target)
                val result = executeTargetPhase(execution, target, phase, force || dirty, dryRun)
                if (result.status == Status.SUCCESS) {
                    dirtyManager.taint(target)
                }
                result
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
    private def executeTargetPhase(execution: Execution, target:Target, phase:Phase, force:Boolean, dryRun:Boolean) : TargetResult = {
        val forceDirty = force || execution.flowmanConf.getConf(FlowmanConf.EXECUTION_TARGET_FORCE_DIRTY)
        val useHistory = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_TARGET_USE_HISTORY)

        // We need to check the target *before* we run code inside the monitor (which will mark the target as RUNNING)
        val isClean = !forceDirty && useHistory && checkTarget(target.digest(phase))

        def isDirty : Trilean = {
            try {
                target.dirty(execution, phase)
            }
            catch {
                case NonFatal(ex) =>
                    logger.warn(yellow(s"Cannot infer dirty status for target '${target.identifier}' because of exception:\n${ExceptionUtils.reasons(ex)}"))
                    Unknown
            }
        }

        val startTime = Instant.now()
        execution.monitorTarget(target, phase) { execution =>
            logSubtitle(s"$phase target '${target.identifier}'")

            // First checkJob if execution is really required
            if (isClean) {
                logger.info(cyan(s"Target '${target.identifier}' up to date for phase '$phase' according to state store, skipping execution"))
                logger.info("")
                TargetResult(target, phase, Status.SKIPPED, startTime)
            }
            else if (!forceDirty && isDirty == No) {
                logger.info(cyan(s"Target '${target.identifier}' not dirty in phase $phase, skipping execution"))
                logger.info("")
                TargetResult(target, phase, Status.SKIPPED, startTime)
            }
            else {
                resultOf(target, phase, dryRun) {
                    target.execute(execution, phase)
                }
            }
        }
    }

    /**
     * Performs some checks, if the target is already up to date
     * @param target
     * @return
     */
    private def checkTarget(target:TargetDigest) : Boolean = {
        val phase = target.phase

        def checkState(state:TargetState) : Boolean = {
            val lifecycle = Lifecycle.ofPhase(phase)
            if (!lifecycle.contains(state.phase)) {
                // Different lifecycle => target is not valid
                false
            } else if (lifecycle.indexOf(state.phase) < lifecycle.indexOf(phase)) {
                // Same lifecycle, but previous phase => target is not valid
                false
            } else {
                state.status.success
            }
        }

        try {
            stateStore.getTargetState(target) match {
                case Some(state: TargetState) => checkState(state)
                case _ => false
            }
        }
        catch {
            case NonFatal(ex) =>
                logger.error("Cannot retrieve status from history database.", ex)
                false
        }
    }
}


/**
 * Private Implementation for Test specific methods
 * @param runner
 */
private[execution] final class TestRunnerImpl(runner:Runner) extends RunnerImpl {
    def executeTest(test:Test, keepGoing:Boolean=false, dryRun:Boolean=false) : Status = {
        runner.withExecution(true) { execution =>
            runner.withTestContext(test, Some(execution), dryRun) { context =>
                val title = s"Running test '${test.identifier}'"
                logTitle(title)
                logEnvironment(context)

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
                        withJobGroup(sc, test.name, s"EXECUTE test ${test.identifier}") {
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
                val status = Status.ofAll(Seq(buildStatus, testStatus, destroyStatus))
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
            withJobGroup(sc, target.name, s"$phase target ${target.identifier}") {
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
    private[execution] val hooks: Seq[Prototype[Hook]]=Seq()
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
    def executeJob(job:Job, phases:Seq[Phase], args:Map[String,Any]=Map(), targets:Seq[Regex]=Seq(".*".r), dirtyTargets:Seq[Regex]=Seq(), force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false, isolated:Boolean=true) : Status = {
        require(args != null)
        require(phases != null)
        require(args != null)

        val runner = new JobRunnerImpl(this)
        val result = runner.executeJob(job, phases, args, targets, dirtyTargets=dirtyTargets, force=force, keepGoing=keepGoing, dryRun=dryRun, isolated=isolated)
        result.status
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
    def executeTargets(targets:Seq[Target], phases:Seq[Phase], jobName:String="execute-target", force:Boolean, keepGoing:Boolean=false, dryRun:Boolean=false, isolated:Boolean=true) : Status = {
        if (targets.nonEmpty) {
            val context = targets.head.context

            // Build a new scoped job context, which contains the given targets. This way, the targets do not need
            // to be part of the project, but they still *can* be part
            val jobContext = ScopeContext.builder(context)
                .withTargets(targets.map(tgt => (tgt.name, Prototype.of(tgt))).toMap)
                .build()
            val job = Job.builder(jobContext)
                .setName(jobName)
                .setTargets(targets.map(_.identifier))
                .setParameters(Seq(Job.Parameter("execution_ts", LongType)))
                .build()

            executeJob(job, phases, args=Map("execution_ts" -> Clock.systemUTC().millis()), force=force, keepGoing=keepGoing, dryRun=dryRun, isolated=isolated)
        }
        else {
            Status.SUCCESS
        }
   }

    def withExecution[T](isolated:Boolean=false)(fn:Execution => T) : T = {
        val execution : Execution = new ScopedExecution(parentExecution, isolated)
        val result = fn(execution)

        // Wait for any running background activities, and do not perform a cleanup
        val ops = execution.activities
        val activeOps = ops.listActive()
        if (activeOps.nonEmpty) {
            logger.info("Some background activities are still active:")
            activeOps.foreach(o => logger.info(s"  - s${o.name}"))
            logger.info("Waiting for termination...")
            ops.awaitTermination()
        }

        // Finally release any resources
        execution.cleanup()
        result
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
    def withJobContext[T](job:Job, args:Map[String,Any]=Map(), execution:Option[Execution]=None, force:Boolean=false, dryRun:Boolean=false, isolated:Boolean=true)(fn:(Context,Map[String,Any]) => T) : T = {
        val arguments : Map[String,Any] = job.parameters.flatMap(p => p.default.map(d => p.name -> d)).toMap ++ args
        arguments.toSeq.sortBy(_._1).foreach { case (k,v) => logger.info(s"Job argument $k=$v")}

        verifyArguments(job,arguments)

        val jobContext =
            if (isolated || arguments.nonEmpty || job.environment.nonEmpty) {
                val rootContext = RootContext.builder(job.context)
                    .withEnvironment("force", force)
                    .withEnvironment("dryRun", dryRun)
                    .withEnvironment("job", JobWrapper(job))
                    .withEnvironment(arguments, SettingLevel.SCOPE_OVERRIDE)
                    .withEnvironment(job.environment, SettingLevel.JOB_OVERRIDE)
                    .withExecution(execution)
                    .build()
                if (job.context.project.nonEmpty)
                    rootContext.getProjectContext(job.context.project.get)
                else
                    rootContext
            }
            else {
                ScopeContext.builder(job.context)
                    .withEnvironment("force", force)
                    .withEnvironment("dryRun", dryRun)
                    .withEnvironment("job", JobWrapper(job))
                    .build()
            }

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
    def withTestContext[T](test:Test, execution:Option[Execution]=None, dryRun:Boolean=false)(fn:(Context) => T) : T = {
        val project = test.project.map(_.name)
        val rootContext = RootContext.builder(test.context)
            .withEnvironment("force", false)
            .withEnvironment("dryRun", dryRun)
            .withEnvironment("test", TestWrapper(test))
            .withEnvironment(test.environment, SettingLevel.JOB_OVERRIDE)
            .withExecution(execution)
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
        withJobContext(job, args, force=force, dryRun=dryRun) { (jobContext,_) =>
            withPhaseContext(jobContext, phase) { context =>
                fn(context.environment)
            }
        }
    }

    def withEnvironment[T](test:Test, dryRun:Boolean)(fn:Environment => T) : T = {
        withTestContext(test, dryRun=dryRun) { context =>
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
