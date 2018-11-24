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

package com.dimajix.flowman.namespace.monitor

import java.nio.file.Files
import java.nio.file.Path

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.types.StringType


class JdbcMonitorTest extends FlatSpec with Matchers with BeforeAndAfter {
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
        val spec =
            """
              |kind: jdbc
              |connection: logger
            """.stripMargin
        val monitor = ObjectMapper.parse[Monitor](spec)

        val job = Job.builder()
            .setName("job")
            .build()
        val ns = Namespace.builder()
            .addConnection("logger", JdbcConnection("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:"+db+";create=true", "", ""))
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        monitor.check(session.context, job, Map()) should be(false)
        val token = monitor.start(session.context, job, Map())
        monitor.check(session.context, job, Map()) should be(false)
        monitor.success(session.context, token)
        monitor.check(session.context, job, Map()) should be(true)
    }

    it should "throw an exception on missing connection" in {
        val db = tempDir.resolve("mydb")
        val spec =
            """
              |kind: jdbc
              |connection: logger
            """.stripMargin
        val monitor = ObjectMapper.parse[Monitor](spec)

        val job = Job.builder()
            .setName("job")
            .build()
        val session = Session.builder()
            .build()

        a[NoSuchElementException] shouldBe thrownBy(monitor.check(session.context, job, Map()))
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: jdbc
              |connection: logger
            """.stripMargin

        val monitor = ObjectMapper.parse[Monitor](spec)
        monitor shouldBe a[JdbcMonitor]
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
        monitor.check(session.context, job, Map("p1" -> "v1")) should be (false)
        val token = monitor.start(session.context, job, Map("p1" -> "v1"))
        monitor.check(session.context, job, Map("p1" -> "v1")) should be (false)
        monitor.success(session.context, token)
        monitor.check(session.context, job, Map("p1" -> "v1")) should be (true)
        monitor.check(session.context, job, Map("p1" -> "v2")) should be (false)
        monitor.check(session.context, job, Map("p2" -> "v1")) should be (false)
        monitor.check(session.context, job, Map()) should be (false)
    }
}
