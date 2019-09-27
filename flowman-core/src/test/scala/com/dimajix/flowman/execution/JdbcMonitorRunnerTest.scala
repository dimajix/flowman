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

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnectionSpec
import com.dimajix.flowman.spec.history.JdbcHistorySpec
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.target.NullTargetSpec
import com.dimajix.flowman.types.StringType


class JdbcMonitorRunnerTest extends FlatSpec with Matchers with BeforeAndAfter {
    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("jdbc_logged_runner_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The JdbcStateStoreSpec" should "work with empty jobs" in {
        val db = tempDir.resolve("mydb")
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnectionSpec("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val batch = Job.builder(session.context)
            .setName("batch")
            .build()

        val monitor = JdbcHistorySpec("logger")
        val runner = new MonitoredRunner(monitor.instantiate(session.context))
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), Map(), force=false) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), Map(), force=false) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), Map(), force=true) should be (Status.SUCCESS)
    }

    it should "be used in a Session" in {
        val db = tempDir.resolve("mydb")
        val monitor = JdbcHistorySpec("logger")
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnectionSpec("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .setStateStore(monitor)
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val batch = Job.builder(session.context)
            .setName("job")
            .build()

        val runner = session.runner
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE)) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), force=false) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), force=true) should be (Status.SUCCESS)
    }

    it should "work with non-empty jobs" in {
        val db = tempDir.resolve("mydb")
        val monitor = JdbcHistorySpec("logger")
        val project = Project.builder()
            .addTarget("t0", NullTargetSpec("t0"))
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnectionSpec("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .setStateStore(monitor)
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .withProject(project)
            .build()

        val batch = Job.builder(session.getContext(project))
            .setName("job")
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE)) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), force=false) should be (Status.SKIPPED)
        runner.executeJob(session.executor, batch, Seq(Phase.CREATE), force=true) should be (Status.SUCCESS)
    }

    it should "catch exceptions" in {
        val db = tempDir.resolve("mydb")
        val stateStore = JdbcHistorySpec("logger")
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnectionSpec("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .setStateStore(stateStore)
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()
        val batch = Job.builder(session.context)
            .setName("failingJob")
            .addParameter("p0", StringType)
            .build()

        val runner = session.runner
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.FAILED)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD)) should be (Status.FAILED)
    }

    it should "support parameters in targets" in {
        val db = tempDir.resolve("mydb")
        val stateStore = JdbcHistorySpec("logger")
        val project = Project.builder()
            .addTarget("t0", NullTargetSpec("t0", Map("p1" -> "$p1")))
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnectionSpec("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .setStateStore(stateStore)
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .withProject(project)
            .build()
        val batch = Job.builder(session.getContext(project))
            .setName("job")
            .addParameter("p1", StringType)
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SKIPPED)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD), Map("p1" -> "v2")) should be (Status.SUCCESS)
        runner.executeJob(session.executor, batch, Seq(Phase.BUILD), Map("p1" -> "v2"), force=true) should be (Status.SUCCESS)
    }
}
