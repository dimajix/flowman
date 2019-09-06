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

        val job = BatchInstance("default", "p1", "j1")

        val store1 = new JdbcStateStore(connection)
        store1.checkJob(job) should be(false)
        val store2 = new JdbcStateStore(connection)
        store2.checkJob(job) should be(false)
    }

    "The Job-API of JdbcStateStore" should "provide basic state management for jobs" in {
        val store = newStateStore()

        val job = BatchInstance("default", "p1", "j1")

        store.checkJob(job) should be(false)
        store.getBatchState(job) should be (None)
        val token = store.startBatch(job, None)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token, Status.SUCCESS)
        store.checkJob(job) should be(true)
        store.getBatchState(job).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on job failures" in {
        val store = newStateStore()

        val job = BatchInstance("default", "p1", "j1")

        store.checkJob(job) should be(false)
        store.getBatchState(job) should be (None)
        val token = store.startBatch(job, None)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token, Status.SUCCESS)
        store.checkJob(job) should be(true)
        store.getBatchState(job).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startBatch(job, None)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token2, Status.FAILED)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return success on skipped jobs" in {
        val store = newStateStore()

        val job = BatchInstance("default", "p1", "j1")

        store.checkJob(job) should be(false)
        store.getBatchState(job) should be (None)
        val token = store.startBatch(job, None)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token, Status.SUCCESS)
        store.checkJob(job) should be(true)
        store.getBatchState(job).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startBatch(job, None)
        store.checkJob(job) should be(false)
        store.getBatchState(job).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token2, Status.SKIPPED)
        store.checkJob(job) should be(true)
        store.getBatchState(job).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support job parameters" in {
        val store = newStateStore()

        val job = BatchInstance("default", "p1", "j1")

        store.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(false)
        val token = store.startBatch(job.copy(args = Map("p1" -> "v1")), None)
        store.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(false)
        store.finishBatch(token, Status.SUCCESS)
        store.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(true)
        store.checkJob(job.copy(args = Map("p1" -> "v2"))) should be(false)
        store.checkJob(job.copy(args = Map("p2" -> "v1"))) should be(false)
        store.checkJob(job) should be(false)
    }


    "The Target-API of JdbcStateStore" should "provide basic state management for targets" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.checkTarget(target) should be(false)
        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, None)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.checkTarget(target) should be(true)
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on target failures" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.checkTarget(target) should be(false)
        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, None)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.checkTarget(target) should be(true)
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, None)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, Status.FAILED)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.FAILED))
    }

    it should "return success on skipped target" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.checkTarget(target) should be(false)
        store.getTargetState(target) should be (None)
        val token = store.startTarget(target, None)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token, Status.SUCCESS)
        store.checkTarget(target) should be(true)
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startTarget(target, None)
        store.checkTarget(target) should be(false)
        store.getTargetState(target).map(_.status) should be (Some(Status.RUNNING))
        store.finishTarget(token2, Status.SKIPPED)
        store.checkTarget(target) should be(true)
        store.getTargetState(target).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "support single value target partitions" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.checkTarget(target.copy(partitions = Map("p1" -> "v1"))) should be(false)
        val token = store.startTarget(target.copy(partitions = Map("p1" -> "v1")), None)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1"))) should be(false)
        store.finishTarget(token, Status.SUCCESS)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1"))) should be(true)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v2"))) should be(false)
        store.checkTarget(target.copy(partitions = Map("p2" -> "v1"))) should be(false)
        store.checkTarget(target) should be(false)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v1"))) should be(false)
    }

    it should "support multi value target partitions" in {
        val store = newStateStore()

        val target = TargetInstance("default", "p1", "j1")

        store.checkTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(false)
        val token = store.startTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2")), None)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(false)
        store.finishTarget(token, Status.SUCCESS)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v2"))) should be(true)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v2", "p2" -> "v2"))) should be(false)
        store.checkTarget(target.copy(partitions = Map("p3" -> "v1", "p2" -> "v2"))) should be(false)
        store.checkTarget(target) should be(false)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1"))) should be(false)
        store.checkTarget(target.copy(partitions = Map("p1" -> "v1", "p2" -> "v1"))) should be(false)
    }
}
