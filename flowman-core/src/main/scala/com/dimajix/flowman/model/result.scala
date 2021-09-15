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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


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
    def map[T,U <: Result](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => U) : Seq[U] = {
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
    def flatMap[T,U <: Result](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => Option[U]) : Seq[U] = {
        val results = mutable.ListBuffer[U]()
        val iter = seq.iterator
        var error = false
        while (iter.hasNext && (!error || keepGoing)) {
            val item = iter.next()
            fn(item) match {
                case Some(result) =>
                    results += result
                    val status = result.status
                    error |= (status != Status.SUCCESS && status != Status.SKIPPED)
                case None =>
            }
        }
        results.toList
    }
}

sealed abstract class Result {
    def name : String
    def category : String
    def kind : String
    def description: Option[String]
    def children : Seq[Result]
    def status : Status = if (exception.isDefined) Status.FAILED else Status.ofAll(children.map(_.status))

    def startTime : Instant
    def endTime : Instant
    def duration : Duration = Duration.between(startTime, endTime)

    def success : Boolean = status == Status.SUCCESS
    def failure : Boolean = status == Status.FAILED
    def skipped : Boolean = status == Status.SKIPPED
    def exception : Option[Throwable] = None

    def numFailures : Int = children.count(_.failure)
    def numSuccesses : Int = children.count(_.success)
    def numExceptions : Int = children.count(_.exception.isDefined) + (if (exception.isDefined) 1 else 0)
}


object LifecycleResult {
    def apply(job:Job, instance: JobInstance, lifecycle: Seq[Phase], status:Status, startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            instance,
            lifecycle,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobInstance, lifecycle: Seq[Phase], children : Seq[Result], startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            instance,
            lifecycle,
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobInstance, lifecycle: Seq[Phase], exception:Throwable, startTime:Instant) : LifecycleResult =
        LifecycleResult(
            job,
            instance,
            lifecycle,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
case class LifecycleResult(
    job: Job,
    instance: JobInstance,
    lifecycle: Seq[Phase],
    override val children : Seq[Result],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def name : String = job.name
    override def category : String = job.category
    override def kind : String = job.kind
    override def description: Option[String] = job.description
}


object JobResult {
    def apply(job:Job, instance: JobInstance, phase: Phase, status:Status, startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            phase,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobInstance, phase: Phase, children : Seq[Result], startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            phase,
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(job:Job, instance: JobInstance, phase: Phase, exception:Throwable, startTime:Instant) : JobResult =
        JobResult(
            job,
            instance,
            phase,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
case class JobResult(
    job: Job,
    instance : JobInstance,
    phase: Phase,
    override val children : Seq[Result],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def name : String = job.name
    override def category : String = job.category
    override def kind : String = job.kind
    override def description: Option[String] = job.description
}


object TargetResult {
    def apply(target:Target, phase: Phase, status:Status, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.instance,
            phase,
            Seq(),
            status,
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, children : Seq[Result], startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.instance,
            phase,
            children,
            Status.ofAll(children.map(_.status)),
            startTime=startTime,
            endTime=Instant.now()
        )
    def apply(target:Target, phase: Phase, exception:Throwable, startTime:Instant) : TargetResult =
        TargetResult(
            target,
            target.instance,
            phase,
            Seq(),
            Status.FAILED,
            Some(exception),
            startTime=startTime,
            endTime=Instant.now()
        )
}
case class TargetResult(
    target: Target,
    instance : TargetInstance,
    phase: Phase,
    override val children : Seq[Result],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def name : String = target.name
    override def category : String = target.category
    override def kind : String = target.kind
    override def description: Option[String] = None
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
case class TestResult(
    test: Test,
    instance : TestInstance,
    override val children : Seq[Result],
    override val status: Status,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def name : String = test.name
    override def category : String = test.category
    override def kind : String = test.kind
    override def description: Option[String] = test.description
}


object AssertionResult {
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
                AssertionResult(assertion, Seq(), Some(exception), startTime, Instant.now())
        }
    }
}
case class AssertionResult(
    assertion: Assertion,
    override val children : Seq[AssertionTestResult],
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def name : String = assertion.name
    override def category : String = assertion.category
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
                AssertionTestResult(name, description, false, Some(exception), startTime, Instant.now())
        }
    }
}
case class AssertionTestResult(
    override val name:String,
    override val description:Option[String],
    override val success:Boolean,
    override val exception: Option[Throwable] = None,
    override val startTime : Instant,
    override val endTime : Instant
) extends Result {
    override def category: String = ""
    override def kind: String = ""
    override def children: Seq[Result] = Seq()
    override def status : Status = {
        if (success)
            Status.SUCCESS
        else
            Status.FAILED
    }
    override def failure: Boolean = !success
}
