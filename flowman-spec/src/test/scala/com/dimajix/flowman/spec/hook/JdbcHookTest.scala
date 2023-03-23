/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.hook

import java.sql.Driver
import java.sql.DriverManager
import java.sql.Statement
import java.time.Instant
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.target.EmptyTarget
import com.dimajix.spark.testing.LocalTempDir


class JdbcHookTest extends AnyFlatSpec with Matchers with LocalTempDir {
    private lazy val db = tempDir.toPath.resolve("mydb")
    private lazy val url = "jdbc:derby:" + db + ";create=true"
    private val driver = "org.apache.derby.jdbc.EmbeddedDriver"

    def withStatement[T](fn:(Statement) => T) : T = {
        DriverRegistry.register(this.driver)
        val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
            case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == this.driver => d
            case d if d.getClass.getCanonicalName == this.driver => d
        }.getOrElse {
            throw new IllegalStateException(
                s"Did not find registered driver with class ${this.driver}")
        }
        val con = driver.connect(url, new Properties())
        try {
            val statement = con.createStatement()
            try {
                fn(statement)
            }
            finally {
                statement.close()
            }
        }
        finally {
            con.close()
        }
    }

    "The JdbcHook" should "be deserializable in a namespace" in {
        val spec =
            s"""
              |hooks:
              |  - kind: jdbc
              |    connection:
              |      kind: jdbc
              |      driver: $driver
              |      url : $url
              |    jobStart: job_start/$$job/$$target
              |    jobFinish: job_finish/$$job/$$target
              |    jobSuccess: job_success/$$job/$$target
              |    jobSkip: job_skip/$$job/$$target
              |    jobFailure: job_failure/$$job/$$target
              |    targetStart: target_start/$$job/$$target
              |    targetFinish: target_finish/$$job/$$target
              |    targetSuccess: target_success/$$job/$$target
              |    targetSkip: target_skip/$$job/$$target
              |    targetFailure: target_failure/$$job/$$target
              |""".stripMargin
        val ns = Namespace.read.string(spec)
        val session = Session.builder()
            .withNamespace(ns)
            .disableSpark()
            .build()
        val hook = session.hooks.head.instantiate(session.context).asInstanceOf[JdbcHook]
        hook.jobStart should be (Some("job_start/$job/$target"))
        hook.jobFinish should be (Some("job_finish/$job/$target"))
        hook.jobSuccess should be (Some("job_success/$job/$target"))
        hook.jobSkip should be (Some("job_skip/$job/$target"))
        hook.jobFailure should be (Some("job_failure/$job/$target"))
        hook.targetStart should be (Some("target_start/$job/$target"))
        hook.targetFinish should be (Some("target_finish/$job/$target"))
        hook.targetSuccess should be (Some("target_success/$job/$target"))
        hook.targetSkip should be (Some("target_skip/$job/$target"))
        hook.targetFailure should be (Some("target_failure/$job/$target"))

        session.shutdown()
    }

    it should "provide a working job API" in {
        val session = Session.builder()
            .withEnvironment("env", "some_environment")
            .disableSpark()
            .build()
        val context = session.context
        val connection:Connection = JdbcConnection(
            Connection.Properties(context),
            url=url,
            driver=driver
        )
        val hook = JdbcHook(
            Hook.Properties(context),
            ConnectionReference(context, Prototype.of(connection)),
            jobStart = Some("INSERT INTO jobs VALUES(1, '$project', '$job', '$phase', 'RUNNING')"),
            jobFinish = Some("INSERT INTO jobs VALUES(2, '$project', '$job', '$phase', '$status')"),
            jobSuccess = Some("INSERT INTO jobs VALUES(3, '$project', '$job', '$phase', '$status')")
        )

        withStatement { statement =>
            statement.executeUpdate(
                """
                  |CREATE TABLE jobs(
                  |     id INT,
                  |     project VARCHAR(30),
                  |     job VARCHAR(30),
                  |     phase VARCHAR(30),
                  |     status VARCHAR(30)
                  |)
                  |""".stripMargin
            )
        }

        val job = Job.builder(context).build()
        val instance = JobDigest("default", "p1", "j1", Phase.BUILD, Map("arg1" -> "v1"))
        val execution = session.execution

        val token = hook.startJob(execution, job, instance, None)
        hook.finishJob(execution, token, JobResult(job, instance, Status.SUCCESS, Instant.now()))

        withStatement { statement =>
            val result = statement.executeQuery("""SELECT * FROM jobs ORDER BY id""")

            result.next() should be (true)
            result.getInt("id") should be (1)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("job") should be ("j1")
            result.getString("status") should be ("RUNNING")

            result.next() should be (true)
            result.getInt("id") should be (2)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("job") should be ("j1")
            result.getString("status") should be ("SUCCESS")

            result.next() should be (true)
            result.getInt("id") should be (3)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("job") should be ("j1")
            result.getString("status") should be ("SUCCESS")

            result.next() should be (false)
            result.close()
        }

        session.shutdown()
    }

    it should "provide a working target API" in {
        val session = Session.builder()
            .withEnvironment("env", "some_environment")
            .disableSpark()
            .build()
        val context = session.context
        val connection:Connection = JdbcConnection(
            Connection.Properties(context),
            url=url,
            driver=driver
        )
        val hook = JdbcHook(
            Hook.Properties(context),
            ConnectionReference(context, Prototype.of(connection)),
            targetStart = Some("INSERT INTO targets VALUES(1, '$project', '$target', '$phase', 'RUNNING')"),
            targetFinish = Some("INSERT INTO targets VALUES(2, '$project', '$target', '$phase', '$status')"),
            targetSuccess = Some("INSERT INTO targets VALUES(3, '$project', '$target', '$phase', '$status')")
        )

        withStatement { statement =>
            statement.executeUpdate(
                """
                  |CREATE TABLE targets(
                  |     id INT,
                  |     project VARCHAR(30),
                  |     target VARCHAR(30),
                  |     phase VARCHAR(30),
                  |     status VARCHAR(30)
                  |)
                  |""".stripMargin
            )
        }

        val target = EmptyTarget(Target.Properties(session.context, "t1"), Map())
        val instance = TargetDigest("default", "p1", "t1", Phase.BUILD, Map("arg1" -> "v1"))
        val execution = session.execution

        val token = hook.startTarget(execution, target, instance, None)
        hook.finishTarget(execution, token, TargetResult(target, instance, Seq(), Status.SUCCESS, None, Instant.now(), Instant.now()))

        withStatement { statement =>
            val result = statement.executeQuery("""SELECT * FROM targets ORDER BY id""")

            result.next() should be (true)
            result.getInt("id") should be (1)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("target") should be ("t1")
            result.getString("status") should be ("RUNNING")

            result.next() should be (true)
            result.getInt("id") should be (2)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("target") should be ("t1")
            result.getString("status") should be ("SUCCESS")

            result.next() should be (true)
            result.getInt("id") should be (3)
            result.getString("phase") should be ("BUILD")
            result.getString("project") should be ("p1")
            result.getString("target") should be ("t1")
            result.getString("status") should be ("SUCCESS")

            result.next() should be (false)
            result.close()
        }

        session.shutdown()
    }
}
