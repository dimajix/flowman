/*
 * Copyright (C) 2021 The Flowman Authors
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

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Success

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult
import com.dimajix.spark.testing.LocalSparkSession


class ParallelExecutorTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ParallelExecutor" should "return SUCCESS on empty lists" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val targets = Seq()

        val executor = new ParallelExecutor(execution, context)
        val result = executor.execute(Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) => TargetResult(target, Phase.BUILD, Status.SUCCESS, start)
        }

        result should be (Seq())

        session.shutdown()
    }

    it should "work" in {
        val session = Session.builder()
            .withConfig(FlowmanConf.EXECUTION_EXECUTOR_PARALLELISM.key, 2)
            .disableSpark()
            .build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val t1 = mock[Target]
        (t1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t1", "default"))
        (t1.name _).expects().atLeastOnce().returns("t1")
        (t1.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t1.requires _).expects(*).atLeastOnce().returns(Set())
        (t1.provides _).expects(*).atLeastOnce().returns(Set())
        (t1.before _).expects().atLeastOnce().returns(Seq())
        (t1.after _).expects().atLeastOnce().returns(Seq())
        (t1.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t1.execute _).expects(*, Phase.BUILD).returns(TargetResult(t1, Phase.BUILD, Status.SUCCESS, start).copy(endTime=start))

        val t2 = mock[Target]
        (t2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t2", "default"))
        (t2.name _).expects().atLeastOnce().returns("t2")
        (t2.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t2.requires _).expects(*).atLeastOnce().returns(Set())
        (t2.provides _).expects(*).atLeastOnce().returns(Set())
        (t2.before _).expects().atLeastOnce().returns(Seq())
        (t2.after _).expects().atLeastOnce().returns(Seq())
        (t2.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t2.execute _).expects(*, Phase.BUILD).returns(TargetResult(t2, Phase.BUILD, Status.SUCCESS, start).copy(endTime=start))

        val targets = Seq(t1, t2)

        val executor = new ParallelExecutor(execution, context)
        val result = executor.execute(Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) =>
                target.execute(execution, phase)
        }

        result.sortBy(_.name) should be (Seq(
            TargetResult(t1, t1.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime=start),
            TargetResult(t2, t2.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime=start)
        ))

        session.shutdown()
    }

    it should "stop on failures" in {
        val session = Session.builder()
            .withConfig(FlowmanConf.EXECUTION_EXECUTOR_PARALLELISM.key, 4)
            .disableSpark()
            .build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val t1 = mock[Target]
        (t1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t1", "default"))
        (t1.name _).expects().atLeastOnce().returns("t1")
        (t1.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t1.requires _).expects(*).atLeastOnce().returns(Set())
        (t1.provides _).expects(*).atLeastOnce().returns(Set())
        (t1.before _).expects().atLeastOnce().returns(Seq())
        (t1.after _).expects().atLeastOnce().returns(Seq())
        (t1.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t1.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            // This future should start only after t3 has been started. Otherwise tests will be flaky
            Thread.sleep(1000)
            TargetResult(t1, Phase.BUILD, Status.FAILED, start).copy(endTime = start)
        }

        val t2 = mock[Target]
        (t2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t2", "default"))
        //(t2.name _).expects().atLeastOnce().returns("t2")
        (t2.requires _).expects(*).atLeastOnce().returns(Set())
        (t2.provides _).expects(*).atLeastOnce().returns(Set())
        (t2.before _).expects().atLeastOnce().returns(Seq())
        (t2.after _).expects().atLeastOnce().returns(Seq(TargetIdentifier("t1", "default")))
        (t2.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        val t3 = mock[Target]
        (t3.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t3", "default"))
        (t3.name _).expects().atLeastOnce().returns("t3")
        (t3.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t3.requires _).expects(*).atLeastOnce().returns(Set())
        (t3.provides _).expects(*).atLeastOnce().returns(Set())
        (t3.before _).expects().atLeastOnce().returns(Seq())
        (t3.after _).expects().atLeastOnce().returns(Seq())
        (t3.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t3.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            Thread.sleep(2000)
            TargetResult(t3, Phase.BUILD, Status.SUCCESS, start).copy(endTime = start)
        }

        val t4 = mock[Target]
        (t4.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t4", "default"))
        //(t4.name _).expects().atLeastOnce().returns("t4")
        (t4.requires _).expects(*).atLeastOnce().returns(Set())
        (t4.provides _).expects(*).atLeastOnce().returns(Set())
        (t4.before _).expects().atLeastOnce().returns(Seq())
        (t4.after _).expects().atLeastOnce().returns(Seq(TargetIdentifier("t3", "default")))
        (t4.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        val targets = Seq(t1, t2, t3, t4)

        val executor = new ParallelExecutor(execution, context)
        val result = executor.execute(Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) =>
                target.execute(execution, phase)
        }

        result.sortBy(_.name) should be(Seq(
            TargetResult(t1, t1.digest(Phase.BUILD), Seq(), Status.FAILED, None, start, start).copy(endTime = start),
            TargetResult(t3, t3.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime = start)
        ))

        session.shutdown()
    }

    it should "ignore failures if keep-going" in {
        val session = Session.builder()
            .withConfig(FlowmanConf.EXECUTION_EXECUTOR_PARALLELISM.key, 2)
            .disableSpark()
            .build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val t1 = mock[Target]
        (t1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t1", "default"))
        (t1.name _).expects().atLeastOnce().returns("t1")
        (t1.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t1.requires _).expects(*).atLeastOnce().returns(Set())
        (t1.provides _).expects(*).atLeastOnce().returns(Set())
        (t1.before _).expects().atLeastOnce().returns(Seq())
        (t1.after _).expects().atLeastOnce().returns(Seq())
        (t1.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t1.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            Thread.sleep(1000)
            TargetResult(t1, Phase.BUILD, Status.FAILED, start).copy(endTime = start)
        }

        val t2 = mock[Target]
        (t2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t2", "default"))
        (t2.name _).expects().atLeastOnce().returns("t2")
        (t2.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t2.requires _).expects(*).atLeastOnce().returns(Set())
        (t2.provides _).expects(*).atLeastOnce().returns(Set())
        (t2.before _).expects().atLeastOnce().returns(Seq())
        (t2.after _).expects().atLeastOnce().returns(Seq(TargetIdentifier("t1", "default")))
        (t2.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t2.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            Thread.sleep(500)
            TargetResult(t2, Phase.BUILD, Status.SUCCESS, start).copy(endTime = start)
        }

        val t3 = mock[Target]
        (t3.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t3", "default"))
        (t3.name _).expects().atLeastOnce().returns("t3")
        (t3.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t3.requires _).expects(*).atLeastOnce().returns(Set())
        (t3.provides _).expects(*).atLeastOnce().returns(Set())
        (t3.before _).expects().atLeastOnce().returns(Seq())
        (t3.after _).expects().atLeastOnce().returns(Seq())
        (t3.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t3.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            Thread.sleep(2000)
            TargetResult(t3, Phase.BUILD, Status.SUCCESS, start).copy(endTime = start)
        }

        val t4 = mock[Target]
        (t4.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t4", "default"))
        (t4.name _).expects().atLeastOnce().returns("t4")
        (t4.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        (t4.requires _).expects(*).atLeastOnce().returns(Set())
        (t4.provides _).expects(*).atLeastOnce().returns(Set())
        (t4.before _).expects().atLeastOnce().returns(Seq())
        (t4.after _).expects().atLeastOnce().returns(Seq())
        (t4.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t4.execute _).expects(*, Phase.BUILD).onCall { (_,_) =>
            Thread.sleep(500)
            TargetResult(t4, Phase.BUILD, Status.SUCCESS, start).copy(endTime = start)
        }

        val targets = Seq(t1, t2, t3, t4)

        val executor = new ParallelExecutor(execution, context)
        val result = executor.execute(Phase.BUILD, targets, _ => true, keepGoing = true) {
            (execution, target, phase) =>
                target.execute(execution, phase)
        }

        result.sortBy(_.name) should be(Seq(
            TargetResult(t1, t1.digest(Phase.BUILD), Seq(), Status.FAILED, None, start, start).copy(endTime = start),
            TargetResult(t2, t2.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime = start),
            TargetResult(t3, t3.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime = start),
            TargetResult(t4, t4.digest(Phase.BUILD), Seq(), Status.SUCCESS, None, start, start).copy(endTime = start)
        ))

        session.shutdown()
    }
}
