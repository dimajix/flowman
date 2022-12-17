/*
 * Copyright 2018 Kaya Kupferschmidt
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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.RunnerJobTest.NullTarget
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobLifecycle
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


object RunnerJobTest {
    object NullTarget {
        def apply(name:String, partition: Map[String,String] = Map()) : Prototype[Target] = Prototype.of(
            (ctx:Context) => NullTarget(Target.Properties(ctx, name), ctx.evaluate(partition))
        )
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


class RunnerJobTest extends AnyFlatSpec with MockFactory with Matchers with LocalSparkSession {
    "The Runner for Jobs" should "correctly handle environments and arguments" in {
        val project = Project(
            name = "default",
            environment = Map(
                "project_env" -> "project",
                "project_env_to_overwrite" -> "project"
            )
        )
        val session = Session.builder()
            .withEnvironment("global_env", "global")
            .withEnvironment("global_env_to_overwrite", "global")
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)
        val job = Job.builder(context)
            .setName("my_job")
            .addParameter("param", StringType)
            .addEnvironment("global_env_to_overwrite", "job")
            .addEnvironment("project_env_to_overwrite", "job")
            .addEnvironment("job_env", "job")
            .build()

        val args = Map(
            "param" -> "lala"
        )

        val runner = session.runner
        runner.withJobContext(job, args, force=false, dryRun=false) { (context,args) =>
            args should be (Map("param" -> "lala"))
            context.environment.toMap should be(Map(
                "param" -> "lala",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "job",
                "global_env" -> "global",
                "global_env_to_overwrite" -> "global",
                "job_env" -> "job",
                "job" -> JobWrapper(job),
                "force" -> false,
                "dryRun" -> false,
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }
        runner.withEnvironment(job, Phase.BUILD, args, force=false, dryRun=false) { environment =>
            environment.toMap should be(Map(
                "param" -> "lala",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "job",
                "global_env" -> "global",
                "global_env_to_overwrite" -> "global",
                "job_env" -> "job",
                "job" -> JobWrapper(job),
                "force" -> false,
                "dryRun" -> false,
                "phase" -> "BUILD",
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }

        session.shutdown()
    }

    it should "work" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)

        session.shutdown()
    }

    it should "throw exceptions on missing parameters" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addParameter("p1", StringType)
            .build()

        val runner = session.runner
        an[IllegalArgumentException] shouldBe thrownBy(runner.executeJob(job, Seq(Phase.BUILD)))

        session.shutdown()
    }

    it should "fail on missing targets" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addTarget(TargetIdentifier("some_target"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.FAILED)

        session.shutdown()
    }

    it should "catch exceptions during execution" in {
        val targetTemplate = mock[Prototype[Target]]
        val target = mock[Target]
        val project = Project(
            "project",
            targets = Map("some_target" -> targetTemplate)
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(project))
            .setName("batch")
            .addTarget(TargetIdentifier("some_target"))
            .build()

        (targetTemplate.instantiate _).expects(*,None).returns(target)
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("project/some_target"))
        (target.name _).expects().atLeastOnce().returns("some_target")
        (target.before _).expects().returns(Seq())
        (target.after _).expects().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD))
        (target.requires _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.provides _).expects(Phase.BUILD).atLeastOnce().returns(Set())
        (target.digest _).expects(Phase.BUILD).atLeastOnce().returns(TargetDigest("default", "project", "some_target", Phase.BUILD))
        (target.dirty _).expects(*, Phase.BUILD).returns(Yes)
        (target.metadata _).expects().atLeastOnce().returns(Metadata(name="some_target", kind="target", category="target"))
        (target.execute _).expects(*, Phase.BUILD).throwing(new UnsupportedOperationException())

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.FAILED)

        session.shutdown()
    }

    it should "only execute specified targets" in {
        def genTarget(name:String, toBeExecuted:Boolean) : Prototype[Target] = Prototype.of {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = mock[Target]
            (target.before _).expects().atLeastOnce().returns(Seq())
            (target.after _).expects().atLeastOnce().returns(Seq())
            (target.phases _).expects().atLeastOnce().returns(Lifecycle.ALL.toSet)
            (target.requires _).expects(*).atLeastOnce().returns(Set())
            (target.provides _).expects(*).atLeastOnce().returns(Set())
            (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier(name))
            if (toBeExecuted) {
                (target.name _).expects().atLeastOnce().returns(name)
                (target.digest _).expects(Phase.CREATE).atLeastOnce().returns(instance)
                (target.dirty _).expects(*, Phase.CREATE).returns(Yes)
                (target.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
                (target.execute _).expects(*, Phase.CREATE).returning(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
            } else {
                (target.execute _).expects(*, Phase.CREATE).never()
            }

            target
        }
        def genJob(session:Session, project:Project) : Job = {
            Job.builder(session.getContext(session.project.get))
                .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
                .build()
        }
        def genProject(targets:Map[String, Boolean]) : Project = {
            Project(
                name = "default",
                targets = targets.map { case(name,toBeExecuted) => name -> genTarget(name, toBeExecuted) }
            )
        }

        {
            val project = genProject(Map("a" -> true, "ax" -> true, "b" -> false))
            val session = Session.builder()
                .withProject(project)
                .withSparkSession(spark)
                .build()
            val job = genJob(session, project)
            val runner = session.runner
            runner.executeJob(job, Seq(Phase.CREATE), targets=Seq("a.*".r)) should be(Status.SUCCESS)

            session.shutdown()
        }
    }

    it should "not execute targets in dryMode" in {
        def genTarget(name:String) : Prototype[Target] = Prototype.of {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = mock[Target]
            (target.name _).expects().atLeastOnce().returns(name)
            (target.before _).expects().atLeastOnce().returns(Seq())
            (target.after _).expects().atLeastOnce().returns(Seq())
            (target.phases _).expects().atLeastOnce().returns(Lifecycle.ALL.toSet)
            (target.requires _).expects(*).atLeastOnce().returns(Set())
            (target.provides _).expects(*).atLeastOnce().returns(Set())
            (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier(name))
            (target.digest _).expects(Phase.CREATE).atLeastOnce().returns(instance)
            (target.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
            (target.dirty _).expects(*, Phase.CREATE).atLeastOnce().returns(Yes)
            (target.execute _).expects(*, Phase.CREATE).never()

            target
        }

        val project = Project(
            name = "default",
            targets = Map("a" -> genTarget("a"))
        )

        val session = Session.builder()
            .withProject(project)
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(session.project.get))
            .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), dryRun = true) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "stop execution in case of an exception" in {
        def genTarget(name:String, throwsException:Boolean, toBeExecuted:Boolean, before:Seq[String]=Seq(), after:Seq[String]=Seq()) : Prototype[Target] = Prototype.of {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = mock[Target]
            (target.before _).expects().atLeastOnce().returns(before.map(TargetIdentifier(_)))
            (target.after _).expects().atLeastOnce().returns(after.map(TargetIdentifier(_)))
            (target.phases _).expects().atLeastOnce().returns(Lifecycle.ALL.toSet)
            (target.requires _).expects(*).atLeastOnce().returns(Set())
            (target.provides _).expects(*).atLeastOnce().returns(Set())
            (target.project _).expects().anyNumberOfTimes().returns(None)
            (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier(name))
            if (toBeExecuted) {
                (target.name _).expects().atLeastOnce().returns(name)
                (target.digest _).expects(Phase.CREATE).atLeastOnce().returns(instance)
                (target.dirty _).expects(*, Phase.CREATE).atLeastOnce().returns(Yes)
                (target.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
                if (throwsException) {
                    (target.execute _).expects(*, Phase.CREATE).throwing(new UnsupportedOperationException)
                }
                else {
                    (target.execute _).expects(*, Phase.CREATE).returning(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
                }
            } else {
                (target.execute _).expects(*, Phase.CREATE).never()
            }

            target
        }

        val project = Project(
            name = "default",
            targets = Map(
                "t0" -> genTarget("t0", true, true),
                "t1" -> genTarget("t1", false, false, after=Seq("t0"))
            )
        )
        val session = Session.builder()
            .withProject(project)
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(session.project.get))
            .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
            .build()
        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE)) should be(Status.FAILED)

        session.shutdown()
    }

    it should "continue execution in case of an exception with keep-going enabled" in {
        def genTarget(name:String, throwsException:Boolean, before:Seq[String]=Seq(), after:Seq[String]=Seq()) : Prototype[Target] = Prototype.of {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = mock[Target]
            (target.name _).expects().atLeastOnce().returns(name)
            (target.before _).expects().atLeastOnce().returns(before.map(TargetIdentifier(_)))
            (target.after _).expects().atLeastOnce().returns(after.map(TargetIdentifier(_)))
            (target.phases _).expects().atLeastOnce().returns(Lifecycle.ALL.toSet)
            (target.requires _).expects(*).atLeastOnce().returns(Set())
            (target.provides _).expects(*).atLeastOnce().returns(Set())
            (target.project _).expects().anyNumberOfTimes().returns(None)
            (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier(name))
            (target.digest _).expects(Phase.CREATE).atLeastOnce().returns(instance)
            (target.dirty _).expects(*, Phase.CREATE).atLeastOnce().returns(Yes)
            (target.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
            if (throwsException) {
                (target.execute _).expects(*, Phase.CREATE).throwing(new UnsupportedOperationException)
            }
            else {
                (target.execute _).expects(*, Phase.CREATE).returning(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
            }

            target
        }

        val project = Project(
            name = "default",
            targets = Map(
                "t0" -> genTarget("t0", true),
                "t1" -> genTarget("t1", false, after=Seq("t0"))
            )
        )
        val session = Session.builder()
            .withProject(project)
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(session.project.get))
            .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
            .build()
        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), keepGoing =true) should be(Status.FAILED)

        session.shutdown()
    }

    it should "execute dependent targets of dirty targets" in {
        def genTarget(name:String, doRun:Boolean, dirty:Trilean, produces:Set[ResourceIdentifier]=Set(), requires:Set[ResourceIdentifier]=Set()) : Prototype[Target] = Prototype.of {
            val instance = TargetDigest("default", "default", name, Phase.CREATE)
            val target = mock[Target]
            (target.name _).expects().atLeastOnce().returns(name)
            (target.before _).expects().atLeastOnce().returns(Seq())
            (target.after _).expects().atLeastOnce().returns(Seq())
            (target.phases _).expects().atLeastOnce().returns(Lifecycle.ALL.toSet)
            (target.requires _).expects(*).atLeastOnce().returns(requires)
            (target.provides _).expects(*).atLeastOnce().returns(produces)
            (target.project _).expects().anyNumberOfTimes().returns(None)
            (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier(name))
            (target.digest _).expects(Phase.CREATE).atLeastOnce().returns(instance)
            (target.dirty _).expects(*, Phase.CREATE).anyNumberOfTimes().returns(dirty)
            (target.metadata _).expects().atLeastOnce().returns(Metadata(name=name, kind="target", category="target"))
            if (doRun) {
                (target.execute _).expects(*, Phase.CREATE).returning(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
            }

            target
        }

        val project = Project(
            name = "default",
            targets = Map(
                "t0" -> genTarget("t0", true, Yes, produces=Set(ResourceIdentifier.ofHivePartition("some_table", Map("p1" -> "123")))),
                "t1" -> genTarget("t1", true, No, requires=Set(ResourceIdentifier.ofHivePartition("some_table", Map.empty[String,Any]))),
                "t2" -> genTarget("t2", false, No)
            )
        )
        val session = Session.builder()
            .withProject(project)
            .withSparkSession(spark)
            .build()
        val job = Job.builder(session.getContext(session.project.get))
            .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
            .build()
        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), keepGoing =true) should be(Status.SUCCESS)

        session.shutdown()
    }

    it should "invoke all hooks (in jobs and namespaces)" in {
        val jobHook = mock[Hook]
        val jobLifecycleToken = new LifecycleToken {}
        val jobJobToken = new JobToken {}
        val jobTargetToken = new TargetToken {}
        (jobHook.startLifecycle _).expects( where( (_:Execution, _:Job, instance:JobLifecycle) => instance.phases == Seq(Phase.BUILD)) ).returning(jobLifecycleToken)
        (jobHook.finishLifecycle _).expects(where( (_:Execution, token:LifecycleToken, result:LifecycleResult) => token == jobLifecycleToken && result.status == Status.SUCCESS))
        (jobHook.startJob _).expects( where( (_:Execution, _:Job, instance:JobDigest, token:Option[Token]) => instance.phase == Phase.BUILD && token == Some(jobLifecycleToken)) ).returning(jobJobToken)
        (jobHook.finishJob _).expects(where( (_:Execution, token:JobToken, result:JobResult) => token == jobJobToken && result.status == Status.SUCCESS))
        (jobHook.startTarget _).expects( where( (_:Execution, _:Target, instance:TargetDigest, token:Option[Token]) => instance.phase == Phase.BUILD && token == Some(jobJobToken))).returning(jobTargetToken)
        (jobHook.finishTarget _).expects(where( (_:Execution, token:TargetToken, result:TargetResult) => token == jobTargetToken && result.status == Status.SUCCESS))
        val namespaceHook = mock[Hook]
        val namespaceLifecycleToken = new LifecycleToken {}
        val namespaceJobToken = new JobToken {}
        val namespaceTargetToken = new TargetToken {}
        (namespaceHook.startLifecycle _).expects( where( (_:Execution, _:Job, instance:JobLifecycle) => instance.phases == Seq(Phase.BUILD)) ).returning(namespaceLifecycleToken)
        (namespaceHook.finishLifecycle _).expects(where( (_:Execution, token:LifecycleToken, result:LifecycleResult) => token == namespaceLifecycleToken && result.status == Status.SUCCESS))
        (namespaceHook.startJob _).expects( where( (_:Execution, _:Job, instance:JobDigest, token:Option[Token]) => instance.phase == Phase.BUILD && token == Some(namespaceLifecycleToken)) ).returning(namespaceJobToken)
        (namespaceHook.finishJob _).expects(where( (_:Execution, token:JobToken, result:JobResult) => token == namespaceJobToken && result.status == Status.SUCCESS))
        (namespaceHook.startTarget _).expects( where( (_:Execution, _:Target, instance:TargetDigest, token:Option[Token]) => instance.phase == Phase.BUILD && token == Some(namespaceJobToken))).returning(namespaceTargetToken)
        (namespaceHook.finishTarget _).expects(where( (_:Execution, token:TargetToken, result:TargetResult) => token == namespaceTargetToken && result.status == Status.SUCCESS))

        val ns = Namespace(
            name = "default",
            hooks = Seq(Prototype.of(namespaceHook))
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> NullTarget("t0", Map("p1" -> "$p1")))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(ns)
            .withProject(project)
            .build()
        val job = Job.builder(session.getContext(project))
            .setName("job")
            .addHook(jobHook)
            .addParameter("p1", StringType)
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SUCCESS)

        session.shutdown()
    }
}
