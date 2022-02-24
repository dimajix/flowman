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
package com.dimajix.flowman.model

import java.time.Duration
import java.time.Instant

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.AssertionRunner
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.AssertionResult.logger


object Result {
    /**
     * Performs a map operation with a function returning some sort of result. The map operation will stop when the
     * first error occurs, except if [[keepGoing]] is set to [[true]]
     * @param seq
     * @param keepGoing
     * @param fn
     * @tparam T
     * @tparam U
     * @return
     */
    def map[T,U <: Result[U]](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => U) : Seq[U] = {
        flatMap(seq, keepGoing)(i => Some(fn(i)))
    }

    /**
     * Performs a flatMap operation with a function returning some sort of result. The map operation will stop when the
     * first error occurs, except if [[keepGoing]] is set to [[true]]
     * @param seq
     * @param keepGoing
     * @param fn
     * @tparam T
     * @tparam U
     * @return
     */
    def flatMap[T,U <: Result[U]](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => Option[U]) : Seq[U] = {
        val results = mutable.ListBuffer[U]()
        val iter = seq.iterator
        var error = false
        while (iter.hasNext && (!error || keepGoing)) {
            val item = iter.next()
            fn(item) match {
                case Some(result) =>
                    results += result
                    val status = result.status
                    error |= status.failure
                case None =>
            }
        }
        results.toList
    }
}

sealed abstract class Result[T <: Result[T]] extends Product with Serializable { this:T =>
    def identifier : Identifier[_]
    def name : String
    def category : Category
    def kind : String
    def description: Option[String]
    def children : Seq[Result[_]]
    def status : Status = if (exception.isDefined) Status.FAILED else Status.ofAll(children.map(_.status))

    def startTime : Instant
    def endTime : Instant
    def duration : Duration = Duration.between(startTime, endTime)

    def success : Boolean = status == Status.SUCCESS || status == Status.SUCCESS_WITH_ERRORS
    def failure : Boolean = status == Status.FAILED
    def skipped : Boolean = status == Status.SKIPPED
    def exception : Option[Throwable] = None

    def numFailures : Int = children.count(_.failure)
    def numSuccesses : Int = children.count(_.success)
    def numExceptions : Int = children.count(_.exception.isDefined) + (if (exception.isDefined) 1 else 0)

    def toTry : Try[Status] = {
        if (exception.nonEmpty)
            Failure(exception.get)
        else
            Success(status)
    }
    /**
     * Rethrows any stored exception
     * @return
     */
    def rethrow() : T = {
        if (exception.nonEmpty)
            throw exception.get
        this
    }
}


object LifecycleResult {
    def apply(job:Job, lifecycle: JobLifecycle, status:Status, startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            lifecycle,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, lifecycle: JobLifecycle, children : Seq[JobResult], startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            lifecycle,
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, lifecycle: JobLifecycle, exception:Throwable, startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            lifecycle,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
final case class LifecycleResult(
    job: Job,
    lifecycle: JobLifecycle,
    override val children : Seq[JobResult],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[LifecycleResult] {
    override def identifier : JobIdentifier = job.identifier
    override def name : String = job.name
    override def category : Category = job.category
    override def kind : String = job.kind
    override def description: Option[String] = job.description
}


object JobResult {
    def apply(job:Job, instance: JobDigest, status:Status) : JobResult =
        JobResult(
            job,
            instance,
            Seq(),
            status,
            startTime=Instant.now(),
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobDigest, status:Status, startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobDigest, children : Seq[Result[_]], startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobDigest, exception:Throwable, startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
final case class JobResult(
    job: Job,
    instance : JobDigest,
    override val children : Seq[Result[_]],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[JobResult] {
    def phase : Phase = instance.phase
    override def identifier : JobIdentifier = job.identifier
    override def name : String = job.name
    override def category : Category = job.category
    override def kind : String = job.kind
    override def description: Option[String] = job.description
}


object TargetResult {
    def apply(target:Target, phase: Phase, status:Status) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            Seq(),
            status,
            startTime=Instant.now(),
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, status:Status, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, children : Seq[Result[_]], startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, children : Seq[Result[_]], status:Status, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            children,
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, children : Seq[Result[_]], exception:Throwable, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            children,
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, exception:Throwable, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.digest(phase),
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )

    def of(target:Target, phase:Phase)(fn: => Unit) : TargetResult = {
        val startTime = Instant.now()
        Try {
            fn
        }
        match {
            case Success(_) => TargetResult(target, phase, Status.SUCCESS, startTime)
            case Failure(ex) => TargetResult(target, phase, ex, startTime)
        }
    }
}
final case class TargetResult(
    target: Target,
    instance : TargetDigest,
    override val children : Seq[Result[_]],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant = Instant.now(),
    override val endTime : Instant = Instant.now()
) extends Result[TargetResult] {
    def phase : Phase = instance.phase
    override def identifier : TargetIdentifier = target.identifier
    override def name : String = target.name
    override def category : Category = target.category
    override def kind : String = target.kind
    override def description: Option[String] = None

    def withoutTime : TargetResult = {
        val ts = Instant.ofEpochSecond(0)
        copy(
            startTime=ts,
            endTime=ts
        )
    }
}


object TestResult {
    def apply(test:Test, status:Status, startTime:Instant) : TestResult =
        TestResult(
            test,
            test.instance,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(test:Test, exception:Throwable, startTime:Instant) : TestResult =
        TestResult(
            test,
            test.instance,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
final case class TestResult(
    test: Test,
    instance : TestInstance,
    override val children : Seq[Result[_]],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[TestResult] {
    override def identifier : TestIdentifier = test.identifier
    override def name : String = test.name
    override def category : Category = test.category
    override def kind : String = test.kind
    override def description: Option[String] = test.description
}


object AssertionResult {
    private val logger = LoggerFactory.getLogger(classOf[AssertionResult])

    def apply(assertion: Assertion, exception:Throwable, startTime:Instant) : AssertionResult =
        AssertionResult(
            assertion,
            Seq(),
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(assertion: Assertion, children : Seq[AssertionTestResult]) : AssertionResult =
        AssertionResult(
            assertion,
            children,
            None,
            startTime=Instant.now(),
            endTime=Instant.now()
        )
    def apply(assertion: Assertion, children : Seq[AssertionTestResult], startTime:Instant) : AssertionResult =
        AssertionResult(
            assertion,
            children,
            None,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(assertion: Assertion, startTime:Instant) : AssertionResult =
        AssertionResult(
            assertion,
            Seq(),
            None,
            startTime=startTime,
            endTime=Instant.now()
        )

    def of(assertion: Assertion)(fn: => Seq[AssertionTestResult]) : AssertionResult = {
        val startTime = Instant.now()
        Try(fn) match {
            case Success(results) =>
                AssertionResult(assertion, results, None, startTime, Instant.now())
            case Failure(exception) =>
                logger.error(s"Caught exception while executing assertion '${assertion.name}': ", exception)
                AssertionResult(assertion, Seq(), Some(exception), startTime, Instant.now())
        }
    }
}
final case class AssertionResult(
    assertion: Assertion,
    override val children : Seq[AssertionTestResult],
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[AssertionResult] {
    override def identifier : AssertionIdentifier = assertion.identifier
    override def name : String = assertion.name
    override def category : Category = assertion.category
    override def kind : String = assertion.kind
    override def description: Option[String] = assertion.description

    def withoutTime : AssertionResult = {
        val ts = Instant.ofEpochSecond(0)
        copy(
            children=children.map(_.copy(startTime=ts, endTime=ts)),
            startTime=ts,
            endTime=ts
        )
    }
}


object AssertionTestResult {
    private val logger = LoggerFactory.getLogger(classOf[AssertionTestResult])

    def apply(name:String, description:Option[String], success:Boolean) : AssertionTestResult = AssertionTestResult(
        name,
        description,
        success,
        None,
        Instant.now(),
        Instant.now()
    )
    def apply(name:String, description:Option[String], success:Boolean, startTime:Instant) : AssertionTestResult = AssertionTestResult(
        name,
        description,
        success,
        None,
        startTime,
        Instant.now()
    )
    def apply(name:String, description:Option[String], exception: Throwable, startTime:Instant) : AssertionTestResult = AssertionTestResult(
        name,
        description,
        false,
        Some(exception),
        startTime,
        Instant.now()
    )

    def of(name: String, description:Option[String]=None)(fn: => Boolean) : AssertionTestResult = {
        val startTime = Instant.now()
        Try(fn) match {
            case Success(result) =>
                AssertionTestResult(name, description, result, None, startTime, Instant.now())
            case Failure(exception) =>
                logger.error(s"Caught exception executing test '${name}': ", exception)
                AssertionTestResult(name, description, false, Some(exception), startTime, Instant.now())
        }
    }
}
final case class AssertionTestResult(
    override val name:String,
    override val description:Option[String],
    override val success:Boolean,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[AssertionTestResult] {
    override def identifier : EmptyIdentifier = EmptyIdentifier.empty
    override def category: Category = Category.ASSERTION_TEST
    override def kind: String = ""
    override def children: Seq[Result[_]] = Seq()
    override def status : Status = {
        if (success)
            Status.SUCCESS
        else
            Status.FAILED
    }
}


object MeasureResult {
    private val logger = LoggerFactory.getLogger(classOf[MeasureResult])

    def apply(assertion: Measure, children : Seq[Measurement]) : MeasureResult =
        MeasureResult(
            assertion,
            children,
            None,
            startTime=Instant.now(),
            endTime=Instant.now()
        )
    def apply(measure: Measure, children : Seq[Measurement], startTime:Instant) : MeasureResult =
        MeasureResult(
            measure,
            children,
            None,
            startTime=startTime,
            endTime=Instant.now()
        )

    def of(measure: Measure)(fn: => Seq[Measurement]) : MeasureResult = {
        val startTime = Instant.now()
        Try(fn) match {
            case Success(results) =>
                MeasureResult(measure, results, None, startTime, Instant.now())
            case Failure(exception) =>
                logger.error(s"Caught exception while executing measure '${measure.name}': ", exception)
                MeasureResult(measure, Seq(), Some(exception), startTime, Instant.now())
        }
    }
}
final case class MeasureResult(
    measure: Measure,
    measurements: Seq[Measurement],
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result[MeasureResult] {
    override def identifier : MeasureIdentifier = measure.identifier
    override def name : String = measure.name
    override def category : Category = measure.category
    override def kind : String = measure.kind
    override def children : Seq[Result[_]] = Seq()
    override def description: Option[String] = measure.description

    def withoutTime : MeasureResult = {
        val ts = Instant.ofEpochSecond(0)
        copy(
            startTime=ts,
            endTime=ts
        )
    }
}


final case class Measurement(
    name:String,
    labels:Map[String,String],
    value:Double
)
