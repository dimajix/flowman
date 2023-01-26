/*
 * Copyright 2021-2023 Kaya Kupferschmidt
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
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.spark.testing.LocalSparkSession


class ManualSchedulerTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ManualScheduler" should "work" in {
        val t1 = mock[Target]
        (t1.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        val t2 = mock[Target]
        (t2.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        val t3 = mock[Target]
        (t3.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE))
        val t4 = mock[Target]
        (t4.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD))
        val t5 = mock[Target]
        (t5.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        val targets = Seq(t1,t2,t3,t4,t5)

        val scheduler = new ManualScheduler()
        scheduler.initialize(targets, Phase.BUILD, _ => true)

        scheduler.hasNext() should be (true)
        scheduler.next() should be (Some(t1))
        scheduler.hasNext() should be (true)
        scheduler.next() should be (Some(t2))
        scheduler.hasNext() should be (true)
        scheduler.next() should be (Some(t4))
        scheduler.hasNext() should be (true)
        scheduler.next() should be (Some(t5))
    }

    it should "work with a job and a runner" in {
        def genTarget(name:String) : (Target,Prototype[Target]) = {
            val t = mock[Target]
            val p = mock[Prototype[Target]]
            (p.instantiate _).expects(*,None).returns(t)
            (t.identifier _).expects().atLeastOnce().returns(TargetIdentifier("prj/" + name))
            (t.name _).expects().atLeastOnce().returns(name)
            (t.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
            (t.requires _).expects(*).atLeastOnce().returns(Set())
            (t.provides _).expects(*).atLeastOnce().returns(Set())
            (t.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "prj", name, Phase.BUILD))
            (t.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
            (t.dirty _).expects(*, Phase.BUILD).returns(Yes)
            (t,p)
        }
        val (t1,t1t) = genTarget("t1")
        val (t2,t2t) = genTarget("t2")
        val (t3,t3t) = genTarget("t3")
        val (t4,t4t) = genTarget("t4")
        val (t5,t5t) = genTarget("t5")

        val project = Project(
            name = "prj",
            targets = Map(
                "t1" -> t1t,
                "t2" -> t2t,
                "t3" -> t3t,
                "t4" -> t4t,
                "t5" -> t5t
            )
        )
        val session = Session.builder()
            .withProject(project)
            .withConfig(FlowmanConf.EXECUTION_SCHEDULER_CLASS.key, classOf[ManualScheduler].getCanonicalName)
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)
        val job = Job.builder(context)
            .setTargets(Seq(
                TargetIdentifier("t1"),
                TargetIdentifier("t2"),
                TargetIdentifier("t3"),
                TargetIdentifier("t4"),
                TargetIdentifier("t5")
            ))
            .build()

        inSequence {
            (t1.execute _).expects(*, Phase.BUILD).returns(TargetResult(t1, Phase.BUILD, Status.SUCCESS, Instant.now()))
            (t2.execute _).expects(*, Phase.BUILD).returns(TargetResult(t2, Phase.BUILD, Status.SUCCESS, Instant.now()))
            (t3.execute _).expects(*, Phase.BUILD).returns(TargetResult(t3, Phase.BUILD, Status.SUCCESS, Instant.now()))
            (t4.execute _).expects(*, Phase.BUILD).returns(TargetResult(t4, Phase.BUILD, Status.SUCCESS, Instant.now()))
            (t5.execute _).expects(*, Phase.BUILD).returns(TargetResult(t5, Phase.BUILD, Status.SUCCESS, Instant.now()))
        }
        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be(Status.SUCCESS)

        session.shutdown()
    }
}
