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

package com.dimajix.flowman.execution

import java.time.Instant

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult
import com.dimajix.spark.testing.LocalSparkSession


class ParallelExecutorTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ParallelExecutor" should "return SUCCESS on empty lists" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val targets = Seq()
        val target = mock[Target]

        val executor = new ParallelExecutor
        val result = executor.execute(execution, context, Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) => TargetResult(target, Phase.BUILD, Status.SUCCESS, start)
        }

        result should be (Seq())
    }

    it should "work" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val execution = session.execution

        val start = Instant.now()
        val t1 = mock[Target]
        (t1.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t1", "default"))
        (t1.name _).expects().atLeastOnce().returns("t1")
        (t1.instance _).expects().atLeastOnce().returns(TargetInstance("", "", "", Map()))
        (t1.requires _).expects(*).atLeastOnce().returns(Set())
        (t1.provides _).expects(*).atLeastOnce().returns(Set())
        (t1.before _).expects().atLeastOnce().returns(Seq())
        (t1.after _).expects().atLeastOnce().returns(Seq())
        (t1.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t1.execute _).expects(*, Phase.BUILD).returns(TargetResult(t1, Phase.BUILD, Status.SUCCESS, start).copy(endTime=start))

        val t2 = mock[Target]
        (t2.identifier _).expects().atLeastOnce().returns(TargetIdentifier("t2", "default"))
        (t2.name _).expects().atLeastOnce().returns("t2")
        (t2.instance _).expects().atLeastOnce().returns(TargetInstance("", "", "", Map()))
        (t2.requires _).expects(*).atLeastOnce().returns(Set())
        (t2.provides _).expects(*).atLeastOnce().returns(Set())
        (t2.before _).expects().atLeastOnce().returns(Seq())
        (t2.after _).expects().atLeastOnce().returns(Seq())
        (t2.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (t2.execute _).expects(*, Phase.BUILD).returns(TargetResult(t2, Phase.BUILD, Status.SUCCESS, start).copy(endTime=start))

        val targets = Seq(t1, t2)

        val executor = new ParallelExecutor
        val result = executor.execute(execution, context, Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) =>
                target.execute(execution, phase)
        }

        result.sortBy(_.name) should be (Seq(
            TargetResult(t1, t1.instance, Phase.BUILD, Seq(), Status.SUCCESS, None, start, start).copy(endTime=start),
            TargetResult(t2, t2.instance, Phase.BUILD, Seq(), Status.SUCCESS, None, start, start).copy(endTime=start)
        ))
    }
}
