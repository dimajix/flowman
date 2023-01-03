/*
 * Copyright 2022 Kaya Kupferschmidt
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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult
import com.dimajix.spark.testing.LocalSparkSession


class RunnerTargetTest extends AnyFlatSpec with MockFactory with Matchers with LocalSparkSession {
    "The Runner for Targets" should "execute unknown targets in non-isolated mode" in {
        val target = mock[Target]
        val project = Project(
            "project"
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)

        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("project/some_target"))
        (target.name _).expects().atLeastOnce().returns("some_target")
        (target.context _).expects().atLeastOnce().returns(context)
        (target.before _).expects().returns(Seq())
        (target.after _).expects().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        (target.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (target.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (target.execute _).expects(*, Phase.BUILD).returns(TargetResult(target, Phase.BUILD, Status.SUCCESS))

        val runner = session.runner
        runner.executeTargets(Seq(target), Seq(Phase.BUILD), jobName="cli-tools", force=true, isolated=false) should be(Status.SUCCESS)
        //runner.executeTargets(Seq(target), Seq(Phase.BUILD), jobName="cli-tools", force=true, isolated=true) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "execute known targets in shared and isolated mode" in {
        val targetTemplate = mock[Prototype[Target]]
        val target = mock[Target]
        val project = Project(
            "project",
            targets = Map("some_target" -> targetTemplate)
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)

        (targetTemplate.instantiate _).expects(*,*).once().returns(target)
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("project/some_target"))
        (target.name _).expects().atLeastOnce().returns("some_target")
        (target.context _).expects().atLeastOnce().returns(context)
        (target.before _).expects().twice().returns(Seq())
        (target.after _).expects().twice().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        (target.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (target.metadata _).expects().atLeastOnce().returns(Metadata(name = "some_target", kind = "target", category = "target"))
        (target.execute _).expects(*, Phase.BUILD).twice().returns(TargetResult(target, Phase.BUILD, Status.SUCCESS))

        val runner = session.runner
        runner.executeTargets(Seq(target), Seq(Phase.BUILD), jobName = "cli-tools", force = true, isolated = false) should be(Status.SUCCESS)
        runner.executeTargets(Seq(target), Seq(Phase.BUILD), jobName = "cli-tools", force = true, isolated = true) should be(Status.SUCCESS)

        session.shutdown()
    }
}
