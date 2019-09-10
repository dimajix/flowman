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
import com.dimajix.flowman.spec.target.BatchInstance
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

    "The Job-API of JdbcStateStore" should "provide basic state management for batches" in {
        val store = newStateStore()

        val batch = BatchInstance("default", "p1", "j1")

        store.getBatchState(batch) should be (None)
        val token = store.startBatch(batch, Phase.BUILD)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token, Status.SUCCESS)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.SUCCESS))
    }

    it should "return failed on batches failures" in {
        val store = newStateStore()

        val batch = BatchInstance("default", "p1", "j1")

        store.getBatchState(batch) should be (None)
        val token = store.startBatch(batch, Phase.BUILD)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token, Status.SUCCESS)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.SUCCESS))

        val token2 = store.startBatch(batch, Phase.BUILD)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.RUNNING))
        store.finishBatch(token2, Status.FAILED)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch).map(_.status) should be (Some(Status.FAILED))
    }

    it should "support batch parameters" in {
        val store = newStateStore()

        val batch = BatchInstance("default", "p1", "j1")

        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))) should be(None)
        val token = store.startBatch(batch.copy(args = Map("p1" -> "v1")), Phase.BUILD)
        store.getBatchState(batch).map(_.phase) should be (Some(Phase.BUILD))
        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))).map(_.phase) should be(Phase.BUILD)
        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))).map(_.status) should be(Status.RUNNING)
        store.finishBatch(token, Status.SUCCESS)
        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))).map(_.phase) should be(Phase.BUILD)
        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))).map(_.status) should be(Status.SUCCESS)
        store.getBatchState(batch.copy(args = Map("p1" -> "v1"))) should be(None)
        store.getBatchState(batch.copy(args = Map("p1" -> "v2"))) should be(None)
        store.getBatchState(batch.copy(args = Map("p2" -> "v1"))) should be(None)
        store.getBatchState(batch) should be(None)
        store.getBatchState(batch) should be(None)
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

    it should "return success on skipped target" in {
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
        store.getTargetState(target).map(_.status) should be (Some(Status.SKIPPED))
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
