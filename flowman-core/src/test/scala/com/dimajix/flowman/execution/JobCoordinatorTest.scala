/*
 * Copyright (C) 2022 The Flowman Authors
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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.types.DateType
import com.dimajix.spark.testing.LocalSparkSession


class JobCoordinatorTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The JobCoordinator" should "work" in {
        val target = mock[Target]
        val targetP = mock[Prototype[Target]]
        val project = Project(
            "project",
            targets = Map("some_target" -> targetP)
        )
        val session = Session.builder().withProject(project).withSparkSession(spark).build()
        val context = session.getContext(project)
        val job = Job(
            Job.Properties(context),
            targets = Seq(TargetIdentifier("some_target"))
        )

        (targetP.instantiate _).expects(*, None).returns(target)
        (target.name _).expects().atLeastOnce().returns("some_target")
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("some_target"))
        (target.before _).expects().atLeastOnce().returns(Seq())
        (target.after _).expects().atLeastOnce().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.BUILD))
        (target.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (target.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.VALIDATE))
        (target.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (target.execute _).expects(*, Phase.VALIDATE).returning(TargetResult(target, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (target.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (target.dirty _).expects(*, Phase.BUILD).returns(Yes)
        (target.execute _).expects(*, Phase.BUILD).returning(TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now()))

        val coord = new JobCoordinator(session)
        coord.execute(job, Lifecycle.ofPhase(Phase.BUILD)) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "support parallel execution" in {
        val target = mock[Target]
        val targetP = mock[Prototype[Target]]
        val project = Project(
            "project",
            targets = Map("some_target" -> targetP)
        )
        val session = Session.builder().withProject(project).withSparkSession(spark).build()
        val context = session.getContext(project)
        val job = Job(
            Job.Properties(context),
            targets = Seq(TargetIdentifier("some_target")),
            parameters = Seq(
                Job.Parameter("pdate", DateType)
            ),
            executions = Seq(
                Job.Execution(Phase.VALIDATE, CyclePolicy.FIRST),
                Job.Execution(Phase.CREATE, CyclePolicy.NEVER),
                Job.Execution(Phase.BUILD, CyclePolicy.ALWAYS),
                Job.Execution(Phase.VERIFY, CyclePolicy.LAST)
            )
        )

        (targetP.instantiate _).expects(*, None).repeated(8).returns(target)
        (target.name _).expects().atLeastOnce().returns("some_target")
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("some_target"))
        (target.before _).expects().atLeastOnce().returns(Seq())
        (target.after _).expects().atLeastOnce().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.BUILD))
        (target.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (target.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.VALIDATE))
        (target.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (target.execute _).expects(*, Phase.VALIDATE).returning(TargetResult(target, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (target.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (target.dirty _).expects(*, Phase.BUILD).repeated(8).returns(Yes)
        (target.execute _).expects(*, Phase.BUILD).repeated(8).returning(TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now()))

        val coord = new JobCoordinator(session, parallelism = 3)
        coord.execute(job, Lifecycle.ofPhase(Phase.VERIFY), job.parseArguments(Map("pdate:start" -> "2022-11-01", "pdate:end" -> "2022-11-09"))) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "support different phase execution policies" in {
        val firstTarget = mock[Target]
        val midTarget = mock[Target]
        val lastTarget = mock[Target]
        val targetP = mock[Prototype[Target]]
        val project = Project(
            "project",
            targets = Map("some_target" -> targetP)
        )
        val session = Session.builder().withProject(project).withSparkSession(spark).build()
        val context = session.getContext(project)
        val job = Job(
            Job.Properties(context),
            targets = Seq(TargetIdentifier("some_target")),
            parameters = Seq(
                Job.Parameter("pdate", DateType)
            ),
            executions = Seq(
                Job.Execution(Phase.VALIDATE, CyclePolicy.FIRST),
                Job.Execution(Phase.CREATE, CyclePolicy.NEVER),
                Job.Execution(Phase.BUILD, CyclePolicy.ALWAYS),
                Job.Execution(Phase.VERIFY, CyclePolicy.LAST)
            )
        )

        (targetP.instantiate _).expects(*, *).atLeastTwice().onCall { (ctx, _) =>
            val pdate = ctx.environment.get("pdate").get
            if (pdate == DateType.parse("2022-11-01"))
                firstTarget
            else if (pdate == DateType.parse("2022-11-10"))
                lastTarget
            else
                midTarget
        }
        (firstTarget.name _).expects().atLeastOnce().returns("some_target")
        (firstTarget.identifier _).expects().atLeastOnce().returns(TargetIdentifier("some_target"))
        (firstTarget.before _).expects().atLeastOnce().returns(Seq())
        (firstTarget.after _).expects().atLeastOnce().returns(Seq())
        (firstTarget.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY))
        (firstTarget.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (firstTarget.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (firstTarget.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (firstTarget.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.VALIDATE))
        (firstTarget.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (firstTarget.execute _).expects(*, Phase.VALIDATE) returning (TargetResult(firstTarget, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (firstTarget.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (firstTarget.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (firstTarget.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (firstTarget.dirty _).expects(*, Phase.BUILD).returns(Yes)
        (firstTarget.execute _).expects(*, Phase.BUILD).returning(TargetResult(firstTarget, Phase.BUILD, Status.SUCCESS, Instant.now()))

        (midTarget.name _).expects().atLeastOnce().returns("some_target")
        (midTarget.identifier _).expects().atLeastOnce().returns(TargetIdentifier("some_target"))
        (midTarget.before _).expects().atLeastOnce().returns(Seq())
        (midTarget.after _).expects().atLeastOnce().returns(Seq())
        (midTarget.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY))
        (midTarget.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (midTarget.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (midTarget.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (midTarget.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (midTarget.dirty _).expects(*, Phase.BUILD).atLeastTwice().returns(Yes)
        (midTarget.execute _).expects(*, Phase.BUILD).atLeastTwice().returning(TargetResult(midTarget, Phase.BUILD, Status.SUCCESS, Instant.now()))

        (lastTarget.name _).expects().atLeastOnce().returns("some_target")
        (lastTarget.identifier _).expects().atLeastOnce().returns(TargetIdentifier("some_target"))
        (lastTarget.before _).expects().atLeastOnce().returns(Seq())
        (lastTarget.after _).expects().atLeastOnce().returns(Seq())
        (lastTarget.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY))
        (lastTarget.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (lastTarget.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (lastTarget.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (lastTarget.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (lastTarget.dirty _).expects(*, Phase.BUILD).returns(Yes)
        (lastTarget.execute _).expects(*, Phase.BUILD).returning(TargetResult(lastTarget, Phase.BUILD, Status.SUCCESS, Instant.now()))
        (lastTarget.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (lastTarget.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (lastTarget.digest _).expects(Phase.VERIFY).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.VERIFY))
        (lastTarget.dirty _).expects(*, Phase.VERIFY).returns(Yes)
        (lastTarget.execute _).expects(*, Phase.VERIFY) returning (TargetResult(lastTarget, Phase.VERIFY, Status.SUCCESS, Instant.now()))

        val coord = new JobCoordinator(session)
        coord.execute(job, Lifecycle.ofPhase(Phase.VERIFY), job.parseArguments(Map("pdate:start" -> "2022-11-01", "pdate:end" -> "2022-11-11"))) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "support target lists per execution phase" in {
        val target1 = mock[Target]
        val target2 = mock[Target]
        val target3 = mock[Target]
        val targetP1 = mock[Prototype[Target]]
        val targetP2 = mock[Prototype[Target]]
        val targetP3 = mock[Prototype[Target]]
        val project = Project(
            "project",
            targets = Map(
                "target1" -> targetP1,
                "target2" -> targetP2,
                "target3" -> targetP3
            )
        )
        val session = Session.builder().withProject(project).withSparkSession(spark).build()
        val context = session.getContext(project)
        val job = Job(
            Job.Properties(context),
            targets = Seq(
                TargetIdentifier("target1"),
                TargetIdentifier("target2"),
                TargetIdentifier("target3")
            ),
            executions = Seq(
                Job.Execution(Phase.VALIDATE, targets=Seq(".*".r)),
                Job.Execution(Phase.CREATE, targets=Seq(".*1".r)),
                Job.Execution(Phase.BUILD, targets=Seq(".*x".r))
            )
        )

        (targetP1.instantiate _).expects(*, *).once().returns(target1)
        (target1.name _).expects().atLeastOnce().returns("target1")
        (target1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target1"))
        (target1.before _).expects().atLeastOnce().returns(Seq())
        (target1.after _).expects().atLeastOnce().returns(Seq())
        (target1.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY))
        (target1.metadata _).expects().atLeastOnce().returns(Metadata(name = "target1", kind = "target", category = "target"))
        (target1.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.VALIDATE))
        (target1.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (target1.execute _).expects(*, Phase.VALIDATE) returning (TargetResult(target1, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (target1.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.CREATE).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.BUILD))
        (target1.dirty _).expects(*, Phase.CREATE).returns(Yes)
        (target1.execute _).expects(*, Phase.CREATE).returning(TargetResult(target1, Phase.CREATE, Status.SUCCESS, Instant.now()))
        (target1.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.VERIFY).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.VERIFY))
        (target1.dirty _).expects(*, Phase.VERIFY).returns(Yes)
        (target1.execute _).expects(*, Phase.VERIFY).returning(TargetResult(target1, Phase.VERIFY, Status.SUCCESS, Instant.now()))

        (targetP2.instantiate _).expects(*, *).once().returns(target2)
        (target2.name _).expects().atLeastOnce().returns("target2")
        (target2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target2"))
        (target2.before _).expects().atLeastOnce().returns(Seq())
        (target2.after _).expects().atLeastOnce().returns(Seq())
        (target2.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD))
        (target2.metadata _).expects().returns(Metadata(name = "target2", kind = "target", category = "target"))
        (target2.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target2.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "target2", Phase.VALIDATE))
        (target2.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (target2.execute _).expects(*, Phase.VALIDATE) returning (TargetResult(target2, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (target2.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target2.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())

        (targetP3.instantiate _).expects(*, *).once().returns(target3)
        (target3.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target3"))
        (target3.before _).expects().atLeastOnce().returns(Seq())
        (target3.after _).expects().atLeastOnce().returns(Seq())
        (target3.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE))
        (target3.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target3.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target3.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())

        val coord = new JobCoordinator(session)
        coord.execute(job, Lifecycle.ofPhase(Phase.VERIFY)) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "only execute selected targets" in {
        val target1 = mock[Target]
        val target2 = mock[Target]
        val target3 = mock[Target]
        val targetP1 = mock[Prototype[Target]]
        val targetP2 = mock[Prototype[Target]]
        val targetP3 = mock[Prototype[Target]]
        val project = Project(
            "project",
            targets = Map(
                "target1" -> targetP1,
                "target2" -> targetP2,
                "target3" -> targetP3
            )
        )
        val session = Session.builder().withProject(project).withSparkSession(spark).build()
        val context = session.getContext(project)
        val job = Job(
            Job.Properties(context),
            targets = Seq(
                TargetIdentifier("target1"),
                TargetIdentifier("target2"),
                TargetIdentifier("target3")
            )
        )

        (targetP1.instantiate _).expects(*, *).once().returns(target1)
        (target1.name _).expects().atLeastOnce().returns("target1")
        (target1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target1"))
        (target1.before _).expects().atLeastOnce().returns(Seq())
        (target1.after _).expects().atLeastOnce().returns(Seq())
        (target1.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.VERIFY))
        (target1.metadata _).expects().atLeastOnce().returns(Metadata(name = "target1", kind = "target", category = "target"))
        (target1.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.VALIDATE).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.VALIDATE))
        (target1.dirty _).expects(*, Phase.VALIDATE).returns(Yes)
        (target1.execute _).expects(*, Phase.VALIDATE) returning (TargetResult(target1, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        (target1.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.CREATE).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.BUILD))
        (target1.dirty _).expects(*, Phase.CREATE).returns(Yes)
        (target1.execute _).expects(*, Phase.CREATE).returning(TargetResult(target1, Phase.CREATE, Status.SUCCESS, Instant.now()))
        (target1.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target1.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target1.digest _).expects(Phase.VERIFY).atLeastOnce().returns(TargetDigest("default", "project", "target1", Phase.VERIFY))
        (target1.dirty _).expects(*, Phase.VERIFY).returns(Yes)
        (target1.execute _).expects(*, Phase.VERIFY).returning(TargetResult(target1, Phase.VERIFY, Status.SUCCESS, Instant.now()))

        (targetP2.instantiate _).expects(*, *).once().returns(target2)
        (target2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target2"))
        (target2.before _).expects().atLeastOnce().returns(Seq())
        (target2.after _).expects().atLeastOnce().returns(Seq())
        (target2.phases _).expects().atLeastOnce().returns(Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD))
        (target2.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target2.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target2.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target2.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())

        (targetP3.instantiate _).expects(*, *).once().returns(target3)
        (target3.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target3"))
        (target3.before _).expects().atLeastOnce().returns(Seq())
        (target3.after _).expects().atLeastOnce().returns(Seq())
        (target3.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE))
        (target3.requires _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.VALIDATE).atLeastOnce().returns(Set())
        (target3.requires _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.CREATE).atLeastOnce().returns(Set())
        (target3.requires _).expects(Phase.VERIFY).atLeastOnce().returns(Set())
        (target3.provides _).expects(Phase.VERIFY).atLeastOnce().returns(Set())

        val coord = new JobCoordinator(session)
        coord.execute(job, Lifecycle.ofPhase(Phase.VERIFY), targets=Seq(".*1".r)) should be(Status.SUCCESS)

        session.shutdown()
    }
}
