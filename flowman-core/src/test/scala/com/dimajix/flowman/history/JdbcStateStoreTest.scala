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

package com.dimajix.flowman.history

import java.nio.file.Files
import java.nio.file.Path

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.FixedGaugeMetric
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult


class JdbcStateStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfter with MockFactory {
    var tempDir: Path = _

    before {
        tempDir = Files.createTempDirectory("jdbc_logged_runner_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    private def newStateStore() = {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )

        new JdbcStateStore(connection)
    }

    "The JdbcStateStore" should "create tables once" in {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )

        val target = TargetInstance("default", "p1", "j1")

        val store1 = new JdbcStateStore(connection)
        store1.getTargetState(target) should be(None)
        val store2 = new JdbcStateStore(connection)
        store2.getTargetState(target) should be(None)
    }

    "The Job-API of JdbcStateStore" should "provide basic state management for jobs" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val context = RootContext.builder(namespace)
            .build()
            .getProjectContext(project)
        val job = Job.builder(context)
            .setName("j1")
            .build()
        val instance = JobInstance("default", "p1", "j1")

        store.getJobState(instance) should be (None)
        val token = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on job failures" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val context = RootContext.builder(namespace)
            .build()
            .getProjectContext(project)
        val job = Job.builder(context)
            .setName("j1")
            .build()
        val instance = JobInstance("default", "p1", "j1")

        store.getJobState(instance) should be (None)
        val token = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token2, JobResult(job, instance, Phase.BUILD, Status.FAILED))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return correct state on skipped target" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val context = RootContext.builder(namespace)
            .build()
            .getProjectContext(project)
        val job = Job.builder(context)
            .setName("j1")
            .build()
        val instance = JobInstance("default", "p1", "j1")

        store.getJobState(instance) should be (None)
        val token = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token2, JobResult(job, instance, Phase.BUILD, Status.SKIPPED))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token3 = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token3, JobResult(job, instance, Phase.BUILD, Status.FAILED))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.FAILED))

        val token4 = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token4, JobResult(job, instance, Phase.BUILD, Status.SKIPPED))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.FAILED))

        val token5 = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token5, JobResult(job, instance, Phase.BUILD, Status.SUCCESS))
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support job parameters" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val context = RootContext.builder(namespace)
            .build()
            .getProjectContext(project)
        val job = Job.builder(context)
            .setName("j1")
            .build()
        val instance = JobInstance("default", "p1", "j1", Map("p1" -> "v1"))

        store.getJobState(instance) should be(None)
        val token = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance.copy(args = Map())) should be(None)
        store.getJobState(instance.copy(args = Map("p1" -> "v3")))should be(None)
        store.getJobState(instance).map(_.status) should be(Some(Status.RUNNING))
        store.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS))
        store.getJobState(instance).map(_.phase) should be(Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be(Some(Status.SUCCESS))
        store.getJobState(instance.copy(args = Map("p1" -> "v2"))) should be(None)
        store.getJobState(instance.copy(args = Map("p2" -> "v1"))) should be(None)
    }


    "The Target-API of JdbcStateStore" should "provide basic state management for targets" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val instance = TargetInstance("default", "p1", "j1")
        val target = mock[Target]
        (target.namespace _).expects().anyNumberOfTimes().returns(Some(namespace))
        (target.project _).expects().anyNumberOfTimes().returns(Some(project))
        (target.name _).expects().anyNumberOfTimes().returns("j1")
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        store.getTargetState(instance) should be (None)
        val token = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on target failures" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val instance = TargetInstance("default", "p1", "j1")
        val target = mock[Target]
        (target.namespace _).expects().anyNumberOfTimes().returns(Some(namespace))
        (target.project _).expects().anyNumberOfTimes().returns(Some(project))
        (target.name _).expects().anyNumberOfTimes().returns("j1")
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        store.getTargetState(instance) should be (None)
        val token = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, TargetResult(target, Phase.BUILD, Status.FAILED))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return correct state on skipped target" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val instance = TargetInstance("default", "p1", "j1")
        val target = mock[Target]
        (target.namespace _).expects().anyNumberOfTimes().returns(Some(namespace))
        (target.project _).expects().anyNumberOfTimes().returns(Some(project))
        (target.name _).expects().anyNumberOfTimes().returns("j1")
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        store.getTargetState(instance) should be (None)
        val token = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, TargetResult(target, Phase.BUILD, Status.SKIPPED))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.SUCCESS))

        val token3 = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token3, TargetResult(target, Phase.BUILD, Status.FAILED))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.FAILED))

        val token4 = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token4, TargetResult(target, Phase.BUILD, Status.SKIPPED))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.FAILED))

        val token5 = store.startTarget(target, instance, Phase.BUILD, None)
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token5, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(instance).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support single value target partitions" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val instance = TargetInstance("default", "p1", "j1")
        val target = mock[Target]
        (target.namespace _).expects().anyNumberOfTimes().returns(Some(namespace))
        (target.project _).expects().anyNumberOfTimes().returns(Some(project))
        (target.name _).expects().anyNumberOfTimes().returns("j1")
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1"))) should be(None)
        val token = store.startTarget(target, instance.copy(partitions = Map("p1" -> "v1")), Phase.BUILD, None)
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1"))).map(_.status) should be(Some(Status.RUNNING))
        store.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1"))).map(_.status) should be(Some(Status.SUCCESS))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v2"))) should be(None)
        store.getTargetState(instance.copy(partitions = Map("p2" -> "v1"))) should be(None)
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(None)
        store.getTargetState(instance) should be(None)
    }

    it should "support multi value target partitions" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val instance = TargetInstance("default", "p1", "j1")
        val target = mock[Target]
        (target.namespace _).expects().anyNumberOfTimes().returns(Some(namespace))
        (target.project _).expects().anyNumberOfTimes().returns(Some(project))
        (target.name _).expects().anyNumberOfTimes().returns("j1")
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(None)
        val token = store.startTarget(target, instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2")), Phase.BUILD, None)
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.status) should be(Some(Status.RUNNING))
        store.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.status) should be(Some(Status.SUCCESS))
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v2", "p2" -> "v2"))) should be(None)
        store.getTargetState(instance.copy(partitions = Map("p1" -> "v2"))) should be(None)
        store.getTargetState(instance) should be(None)
    }

    it should "support metrics" in {
        val store = newStateStore()

        val namespace = Namespace(name="default")
        val project = Project(name="p1")
        val context = RootContext.builder(namespace)
            .build()
            .getProjectContext(project)
        val job = Job.builder(context)
            .setName("j1")
            .build()
        val instance = JobInstance("default", "p1", "j1")
        val metrics = Measurement.ofMetrics(Seq(
            FixedGaugeMetric("num_records", Map("project" -> "great", "namespace" -> "default", "param" -> "1"), 1.0),
            FixedGaugeMetric("num_records", Map("project" -> "great", "namespace" -> "default", "param" -> "2"), 2.0),
            FixedGaugeMetric("time", Map("project" -> "great", "namespace" -> "default"), 3.0)
        ))

        store.getJobState(instance) should be(None)
        val token = store.startJob(job, instance, Phase.BUILD)
        store.getJobState(instance).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be(Some(Status.RUNNING))
        store.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS), metrics)
        store.getJobState(instance).map(_.phase) should be(Some(Phase.BUILD))
        store.getJobState(instance).map(_.status) should be(Some(Status.SUCCESS))

        val jobId = store.findJobStates(JobQuery()).head.id
        store.getJobMetrics(jobId).sortBy(_.value) should be (metrics.sortBy(_.value))
    }
}
