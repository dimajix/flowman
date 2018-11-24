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

import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.namespace.monitor.JdbcMonitor
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.JobStatus
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

    "The JdbcMonitor" should "work" in {
        val db = tempDir.resolve("mydb")
        val job = Job.builder()
            .setName("job")
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnection("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val monitor = JdbcMonitor("logger")
        val runner = new MonitoredRunner(monitor)
        runner.execute(session.executor, job) should be (JobStatus.SUCCESS)
        runner.execute(session.executor, job) should be (JobStatus.SKIPPED)
        runner.execute(session.executor, job, force=true) should be (JobStatus.SUCCESS)
    }

    it should "be used in a Session" in {
        val db = tempDir.resolve("mydb")
        val monitor = JdbcMonitor("logger")
        val job = Job.builder()
            .setName("job")
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnection("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .setMonitor(monitor)
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val runner = session.runner
        runner.execute(session.executor, job) should be (JobStatus.SUCCESS)
        runner.execute(session.executor, job) should be (JobStatus.SKIPPED)
        runner.execute(session.executor, job, force=true) should be (JobStatus.SUCCESS)
    }

    it should "catch exceptions" in {
        val db = tempDir.resolve("mydb")
        val job = Job.builder()
            .setName("failingJob")
            .addParameter("p0", StringType)
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnection("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val monitor = JdbcMonitor("logger")
        val runner = new MonitoredRunner(monitor)
        runner.execute(session.executor, job) should be (JobStatus.FAILURE)
        runner.execute(session.executor, job) should be (JobStatus.FAILURE)
    }

    it should "support parameters" in {
        val db = tempDir.resolve("mydb")
        val job = Job.builder()
            .setName("job")
            .addParameter("p1", StringType)
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnection("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val monitor = JdbcMonitor("logger")
        val runner = new MonitoredRunner(monitor)
        runner.execute(session.executor, job, Map("p1" -> "v1")) should be (JobStatus.SUCCESS)
        runner.execute(session.executor, job, Map("p1" -> "v1")) should be (JobStatus.SKIPPED)
        runner.execute(session.executor, job, Map("p1" -> "v2")) should be (JobStatus.SUCCESS)
        runner.execute(session.executor, job, Map("p1" -> "v2"), force=true) should be (JobStatus.SUCCESS)
    }
}
