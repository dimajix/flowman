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

import java.time.Instant

import scala.util.Random

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.EXECUTION_TARGET_FORCE_DIRTY
import com.dimajix.flowman.execution.RunnerHistoryTest.NullTarget
import com.dimajix.flowman.history.JdbcStateStore
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


object RunnerHistoryTest {
    object NullTarget {
        def apply(name:String, partition: Map[String,String] = Map()) : Context => NullTarget = {
            ctx:Context => NullTarget(Target.Properties(ctx, name, "null"), ctx.evaluate(partition))
        }
    }
    case class NullTarget(
        instanceProperties: Target.Properties,
        partition: Map[String,String]
    ) extends BaseTarget {
        override def digest(phase:Phase): TargetDigest = {
            TargetDigest(
                namespace.map(_.name).getOrElse(""),
                project.map(_.name).getOrElse(""),
                name,
                phase,
                partition
            )
        }
    }
}


class RunnerHistoryTest extends AnyFlatSpec with MockFactory with Matchers with LocalSparkSession {
    "The Runner with History" should "work with empty jobs" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
            .withNamespace(ns)
            .withSparkSession(spark)
            .build()

        val job = Job.builder(session.context)
            .setName("batch")
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), force=false) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.CREATE), force=false) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.CREATE), force=true) should be (Status.SUCCESS)
    }

    it should "be used in a Session" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
            .withNamespace(ns)
            .withSparkSession(spark)
            .build()

        val job = Job.builder(session.context)
            .setName("job")
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), force=false) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.CREATE), force=false) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.CREATE), force=true) should be (Status.SUCCESS)
    }

    it should "work with non-empty jobs" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> NullTarget("t0"))
        )
        val session = Session.builder()
            .withNamespace(ns)
            .withProject(project)
            .withSparkSession(spark)
            .build()

        val job = Job.builder(session.getContext(project))
            .setName("job")
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE)) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.CREATE), force=false) should be (Status.SKIPPED)
        runner.executeJob(job, Seq(Phase.CREATE), force=true) should be (Status.SUCCESS)
    }

    it should "correctly handle non-dirty targets" in {
        def genTarget(name:String, dirty:Trilean) : Context => Target = (ctx:Context) => {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = stub[Target]
            (target.name _).when().returns(name)
            (target.kind _).when().returns("mock")
            (target.before _).when().returns(Seq())
            (target.after _).when().returns(Seq())
            (target.phases _).when().returns(Lifecycle.ALL.toSet)
            (target.metadata _).when().returns(Metadata(name=name, kind="target", category="target"))
            (target.requires _).when(*).returns(Set())
            (target.provides _).when(*).returns(Set())
            (target.identifier _).when().returns(TargetIdentifier(name))
            (target.digest _).when(Phase.CREATE).returns(instance)
            (target.namespace _).when().returns(ctx.namespace)
            (target.project _).when().returns(ctx.project)
            (target.dirty _).when(*, Phase.CREATE).returns(dirty)
            (target.execute _).when(*, Phase.CREATE).returns(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
            target
        }
        def genJob(session:Session, target:String) : Job = {
            Job.builder(session.getContext(session.project.get))
                .setName("default")
                .addTarget(TargetIdentifier(target))
                .build()
        }

        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val project = Project(
            name = "default",
            targets = Map(
                "dirty0" -> genTarget("dirty0", Yes),
                "clean0" -> genTarget("clean0", No),
                "dirty1" -> genTarget("dirty1", Yes),
                "clean1" -> genTarget("clean1", No)
            )
        )

        {
            val session = Session.builder()
                .withNamespace(ns)
                .withProject(project)
                .withSparkSession(spark)
                .build()
            val runner = session.runner
            runner.executeJob(genJob(session, "clean0"), Seq(Phase.CREATE)) should be(Status.SKIPPED)
            runner.executeJob(genJob(session, "clean0"), Seq(Phase.CREATE), force=true) should be(Status.SUCCESS)
            runner.executeJob(genJob(session, "clean0"), Seq(Phase.CREATE)) should be(Status.SKIPPED)
            runner.executeJob(genJob(session, "dirty0"), Seq(Phase.CREATE)) should be(Status.SUCCESS)
            runner.executeJob(genJob(session, "dirty0"), Seq(Phase.CREATE)) should be(Status.SKIPPED)
            runner.executeJob(genJob(session, "dirty0"), Seq(Phase.CREATE), force=true) should be(Status.SUCCESS)
        }
        {
            val session = Session.builder()
                .withNamespace(ns)
                .withConfig(EXECUTION_TARGET_FORCE_DIRTY.key, "true")
                .withProject(project)
                .withSparkSession(spark)
                .build()
            val runner = session.runner
            runner.executeJob(genJob(session, "clean1"), Seq(Phase.CREATE)) should be(Status.SUCCESS)
            runner.executeJob(genJob(session, "clean1"), Seq(Phase.CREATE)) should be(Status.SKIPPED)
            runner.executeJob(genJob(session, "clean1"), Seq(Phase.CREATE), force=true) should be(Status.SUCCESS)
            runner.executeJob(genJob(session, "dirty1"), Seq(Phase.CREATE)) should be(Status.SUCCESS)
            runner.executeJob(genJob(session, "dirty1"), Seq(Phase.CREATE)) should be(Status.SKIPPED)
            runner.executeJob(genJob(session, "dirty1"), Seq(Phase.CREATE), force=true) should be(Status.SUCCESS)
        }
    }

    it should "catch exceptions" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(ns)
            .build()
        val batch = Job.builder(session.context)
            .setName("failingJob")
            .addTarget(TargetIdentifier("no_such_target"))
            .build()

        val runner = session.runner
        runner.executeJob(batch, Seq(Phase.BUILD)) should be (Status.FAILED)
        runner.executeJob(batch, Seq(Phase.BUILD)) should be (Status.FAILED)
    }

    it should "support parameters in targets" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", driver="org.apache.derby.jdbc.EmbeddedDriver")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> NullTarget("t0", Map("p1" -> "$p1")))
        )
        val session = Session.builder()
            .withNamespace(ns)
            .withProject(project)
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(project))
            .setName("job")
            .addParameter("p1", StringType)
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SKIPPED)
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v2")) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v2"), force=true) should be (Status.SUCCESS)
    }
}
