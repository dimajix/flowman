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

package com.dimajix.flowman.state

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

    "The JdbcStateStore" should "work" in {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )
        val monitor = new JdbcStateStore(connection)

        val job = JobInstance("default", "p1", "j1")

        monitor.checkJob(job) should be(false)
        val token = monitor.startJob(job)
        monitor.checkJob(job) should be(false)
        monitor.success(token)
        monitor.checkJob(job) should be(true)
    }

    it should "support parameters" in {
        val db = tempDir.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )
        val monitor = new JdbcStateStore(connection)

        val job = JobInstance("default", "p1", "j1")

        monitor.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(false)
        val token = monitor.startJob(job.copy(args = Map("p1" -> "v1")))
        monitor.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(false)
        monitor.success(token)
        monitor.checkJob(job.copy(args = Map("p1" -> "v1"))) should be(true)
        monitor.checkJob(job.copy(args = Map("p1" -> "v2"))) should be(false)
        monitor.checkJob(job.copy(args = Map("p2" -> "v1"))) should be(false)
        monitor.checkJob(job) should be(false)
    }
}
