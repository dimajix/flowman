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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


sealed abstract class Result {
    def name : String
    def category : String
    def kind : String
    def description: Option[String]
    def children : Seq[Result]
    def status : Status = Status.ofAll(children.map(_.status))

    def success : Boolean = status == Status.SUCCESS
    def failure : Boolean = status == Status.FAILED
    def skipped : Boolean = status == Status.SKIPPED
    def exception : Boolean = false

    def numFailures : Int = children.count(_.failure)
    def numSuccesses : Int = children.count(_.success)
    def numExceptions : Int = children.count(_.exception)
}

object JobResult {
    def apply(job:Job, instance: JobInstance, phase: Phase, status:Status) : JobResult =
        JobResult(
            job,
            instance,
            phase,
            Seq(),
            status
        )
}
case class JobResult(
    job: Job,
    instance : JobInstance,
    phase: Phase,
    override val children : Seq[Result],
    override val status: Status)
extends Result {
    override def name : String = job.name
    override def category : String = job.category
    override def kind : String = job.kind
    override def description: Option[String] = job.description
}

object TargetResult {
    def apply(target:Target, phase: Phase, status:Status) : TargetResult =
        TargetResult(
            target,
            target.instance,
            phase,
            Seq(),
            status
        )
}
case class TargetResult(
    target: Target,
    instance : TargetInstance,
    phase: Phase,
    override val children : Seq[Result],
    override val status: Status
) extends Result {
    override def name : String = target.name
    override def category : String = target.category
    override def kind : String = target.kind
    override def description: Option[String] = None
}

object TestResult {
    def apply(test:Test, status:Status) : TestResult =
        TestResult(
            test,
            test.instance,
            Seq(),
            status
        )
}
case class TestResult(
    test: Test,
    instance : TestInstance,
    override val children : Seq[Result],
    override val status: Status
) extends Result {
    override def name : String = test.name
    override def category : String = test.category
    override def kind : String = test.kind
    override def description: Option[String] = test.description
}

case class AssertionResult(
    assertion: Assertion,
    override val children : Seq[AssertionTestResult]
) extends Result {
    override def name : String = assertion.name
    override def category : String = assertion.category
    override def kind : String = assertion.kind
    override def description: Option[String] = assertion.description
}

case class AssertionTestResult(
    override val name:String,
    override val description:Option[String],
    override val success:Boolean,
    override val exception:Boolean=false
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
