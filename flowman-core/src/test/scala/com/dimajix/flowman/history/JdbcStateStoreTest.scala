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

package com.dimajix.flowman.history

import java.nio.file.Files
import java.nio.file.Path

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.job.JobInstance
import com.dimajix.flowman.spec.target.TargetInstance


class JdbcStateStoreTest extends FlatSpec with Matchers with BeforeAndAfter {
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

        val job = JobInstance("default", "p1", "j1")

        store.getJobState(job) should be (None)
        val token = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, Status.SUCCESS)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on job failures" in {
        val store = newStateStore()

        val job = JobInstance("default", "p1", "j1")

        store.getJobState(job) should be (None)
        val token = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, Status.SUCCESS)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token2, Status.FAILED)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return correct state on skipped target" in {
        val store = newStateStore()

        val job = JobInstance("default", "p1", "j1")

        store.getJobState(job) should be (None)
        val token = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token, Status.SUCCESS)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token2, Status.SKIPPED)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.SUCCESS))

        val token3 = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token3, Status.FAILED)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.FAILED))

        val token4 = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token4, Status.SKIPPED)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.FAILED))

        val token5 = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishJob(token5, Status.SUCCESS)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support job parameters" in {
        val store = newStateStore()

        val job = JobInstance("default", "p1", "j1", Map("p1" -> "v1"))

        store.getJobState(job) should be(None)
        val token = store.startJob(job, Phase.BUILD)
        store.getJobState(job).map(_.phase) should be (Some(Phase.BUILD))
        store.getJobState(job.copy(args = Map())) should be(None)
        store.getJobState(job.copy(args = Map("p1" -> "v3")))should be(None)
        store.getJobState(job).map(_.status) should be(Some(Status.RUNNING))
        store.finishJob(token, Status.SUCCESS)
        store.getJobState(job).map(_.phase) should be(Some(Phase.BUILD))
        store.getJobState(job).map(_.status) should be(Some(Status.SUCCESS))
        store.getJobState(job.copy(args = Map("p1" -> "v2"))) should be(None)
        store.getJobState(job.copy(args = Map("p2" -> "v1"))) should be(None)
    }


    "The Target-API of JdbcStateStore" should "provide basic state management for targets" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on target failures" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, Status.FAILED)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return correct state on skipped target" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, Status.SKIPPED)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))

        val token3 = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token3, Status.FAILED)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.FAILED))

        val token4 = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token4, Status.SKIPPED)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.FAILED))

        val token5 = store.startTarget(target, Phase.BUILD, None)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token5, Status.SUCCESS)
        store.getTargetState(target).map(_.phase) should be (Some(Phase.BUILD))
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support single value target partitions" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.getTargetState(target.copy(partitions = Map("p1" -> "v1"))) should be(None)
        val token = store.startTarget(target.copy(partitions = Map("p1" -> "v1")), Phase.BUILD, None)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1"))).map(_.status) should be(Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1"))).map(_.status) should be(Some(Status.SUCCESS))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v2"))) should be(None)
        store.getTargetState(target.copy(partitions = Map("p2" -> "v1"))) should be(None)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(None)
        store.getTargetState(target) should be(None)
    }

    it should "support multi value target partitions" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(None)
        val token = store.startTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2")), Phase.BUILD, None)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.status) should be(Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.phase) should be(Some(Phase.BUILD))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))).map(_.status) should be(Some(Status.SUCCESS))
        store.getTargetState(target.copy(partitions = Map("p1" -> "v2", "p2" -> "v2"))) should be(None)
        store.getTargetState(target.copy(partitions = Map("p1" -> "v2"))) should be(None)
        store.getTargetState(target) should be(None)
    }
}
