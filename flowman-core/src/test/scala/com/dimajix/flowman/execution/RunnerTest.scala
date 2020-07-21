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

import java.nio.file.Files
import java.nio.file.Path

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.history.JdbcStateStore
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.types.StringType


class RunnerTest extends FlatSpec with MockFactory with Matchers with BeforeAndAfter {
    object NullTarget {
        def apply(name:String, partition: Map[String,String] = Map()) : Context => NullTarget = {
            ctx:Context => NullTarget(Target.Properties(ctx, name), ctx.evaluate(partition))
        }
    }
    case class NullTarget(
        instanceProperties: Target.Properties,
        partition: Map[String,String]
    ) extends BaseTarget {
        override def instance: TargetInstance = {
            TargetInstance(
                namespace.map(_.name).getOrElse(""),
                project.map(_.name).getOrElse(""),
                name,
                partition
            )
        }
    }

    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("jdbc_logged_runner_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The Runner" should "correctly handle environments and arguments" in {
        val session = Session.builder()
            .withEnvironment("param", "global")
            .withEnvironment("global_env", "global")
            .build()
        val context = session.context
        val job = Job.builder(context)
            .setName("my_job")
            .addParameter("param", StringType)
            .addEnvironment("global_env", "job")
            .addEnvironment("job_env", "job")
            .build()

        val args = Map(
            "param" -> "lala"
        )

        val runner = session.runner
        runner.withEnvironment(job, Phase.BUILD, args, force=false) { environment =>
            environment.toMap should be(Map(
                "param" -> "lala",
                "global_env" -> "global",
                "job_env" -> "job",
                "job" -> "my_job",
                "force" -> false,
                "phase" -> "build"
            ))
        }
    }

    it should "work" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)
    }

    it should "throw exceptions on missing parameters" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addParameter("p1", StringType)
            .build()

        val runner = session.runner
        an[IllegalArgumentException] shouldBe thrownBy(runner.executeJob(job, Seq(Phase.BUILD)))
    }

    it should "catch exceptions" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addTarget(TargetIdentifier("some_target"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.FAILED)
    }

    "The JdbcStateStore" should "work with empty jobs" in {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", "org.apache.derby.jdbc.EmbeddedDriver", "", "")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
            .withNamespace(ns)
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
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", "org.apache.derby.jdbc.EmbeddedDriver", "", "")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
            .withNamespace(ns)
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
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", "org.apache.derby.jdbc.EmbeddedDriver", "", "")
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

    it should "catch exceptions" in {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", "org.apache.derby.jdbc.EmbeddedDriver", "", "")
        val ns = Namespace(
            name = "default",
            history = Some(JdbcStateStore(connection))
        )
        val session = Session.builder()
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
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection("jdbc:derby:"+db+";create=true", "org.apache.derby.jdbc.EmbeddedDriver", "", "")
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

    it should "invoke all hooks (in jobs and namespaces)" in {
        val jobHook = mock[Hook]
        val jobJobToken = new JobToken {}
        val jobTargetToken = new TargetToken {}
        (jobHook.startJob _).expects( where( (_:JobInstance, phase:Phase) => phase == Phase.BUILD) ).returning(jobJobToken)
        (jobHook.finishJob _).expects(jobJobToken, Status.SUCCESS)
        (jobHook.startTarget _).expects( where( (_:TargetInstance, phase:Phase, token:Option[JobToken]) => phase == Phase.BUILD && token == Some(jobJobToken))).returning(jobTargetToken)
        (jobHook.finishTarget _).expects(jobTargetToken, Status.SUCCESS)
        val namespaceHook = mock[Hook]
        val namespaceJobToken = new JobToken {}
        val namespaceTargetToken = new TargetToken {}
        (namespaceHook.startJob _).expects( where( (_:JobInstance, phase:Phase) => phase == Phase.BUILD) ).returning(namespaceJobToken)
        (namespaceHook.finishJob _).expects(namespaceJobToken, Status.SUCCESS)
        (namespaceHook.startTarget _).expects( where( (_:TargetInstance, phase:Phase, token:Option[JobToken]) => phase == Phase.BUILD && token == Some(namespaceJobToken))).returning(namespaceTargetToken)
        (namespaceHook.finishTarget _).expects(namespaceTargetToken, Status.SUCCESS)

        val ns = Namespace(
            name = "default",
            hooks = Seq(new Template[Hook] {
                override def instantiate(context: Context): Hook = namespaceHook
            })
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> NullTarget("t0", Map("p1" -> "$p1")))
        )
        val session = Session.builder()
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
    }
}
