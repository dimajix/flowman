/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.target.JdbcCommandTarget.Action
import com.dimajix.spark.testing.LocalSparkSession
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Status


class JdbcCommandTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    def withDatabase[T](driverClass: String, url: String)(fn: (Statement) => T): T = {
        DriverRegistry.register(driverClass)
        val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
            case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
            case d if d.getClass.getCanonicalName == driverClass => d
        }.getOrElse {
            throw new IllegalStateException(
                s"Did not find registered driver with class $driverClass")
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

    "A JdbcCommandTarget" should "be parseable" in {
        val yaml =
            """
              |kind: jdbcCommand
              |connection: sql_server
              |# Create Fulltext Catalog
              |create:
              |  # Check that catalog does not already exists
              |  condition: |
              |    SELECT 1 FROM sys.fulltext_catalogs
              |    WHERE name = 'ftcat'
              |    HAVING COUNT(*) = 0
              |  sql: |
              |    CREATE FULLTEXT CATALOG ftcat
              |build:
              |  sql:
              |    - INSERT INTO dbo.tweets SELECT * FROM raw_tweets
              |    - ALTER FULLTEXT INDEX ON dbo.tweets START FULL POPULATION
              |# Remove fulltext catalog
              |destroy:
              |  # Check that catalog really exists
              |  condition: |
              |    SELECT 1 FROM sys.fulltext_catalogs
              |    WHERE name = 'ftcat'
              |    HAVING COUNT(*) = 1
              |  sql: |
              |    DROP FULLTEXT CATALOG ftcat
              |""".stripMargin
        val spec = ObjectMapper.parse[TargetSpec](yaml)
        spec shouldBe a[JdbcCommandTargetSpec]
    }

    it should "work" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:derby:" + db + ";create=true"
        val driver = "org.apache.derby.jdbc.EmbeddedDriver"
        val connection = JdbcConnection(
            Connection.Properties(context, "jdbc"),
            url,
            driver
        )
        val target = JdbcCommandTarget(
            Target.Properties(context, "jdbc"),
            connection = ConnectionReference(context, Prototype.of(connection.asInstanceOf[Connection])),
            validateAction = Some(Action(Seq("CREATE TABLE t_validate(str_col CLOB)"),Some("VALUES(1)"))),
            createAction = Some(Action(Seq("CREATE TABLE t_create(str_col CLOB)"),Some("VALUES(0)"))),
            buildAction = Some(Action(Seq("CREATE TABLE t_build(str_col CLOB)"),None)),
            verifyAction = Some(Action(Seq("CREATE TABLE t_verify(str_col CLOB)"),Some("SELECT 1 FROM (VALUES(1)) x WHERE 1 = 0"))),
            truncateAction = Some(Action(Seq("CREATE TABLE t_truncate(str_col CLOB)"),Some("ILLEGAL SQL"))),
            destroyAction = Some(Action(Seq("CREATE TABLE t_destroy(str_col CLOB)"),Some("VALUES(2)")))
        )

        // ========== VALIDATE ========================================================================================
        target.dirty(execution, Phase.VALIDATE) should be (Yes)
        val result_validate1 = target.execute(execution, Phase.VALIDATE)
        result_validate1.status should be (Status.SUCCESS)
        result_validate1.exception should be (None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_validate").close()
        })
        val result_validate2 = target.execute(execution, Phase.VALIDATE)
        result_validate2.status should be(Status.FAILED)
        result_validate2.exception.get shouldBe a[SQLException]

        // ========== CREATE ==========================================================================================
        target.dirty(execution, Phase.CREATE) should be (No)
        val result_create1 = target.execute(execution, Phase.CREATE)
        result_create1.status should be(Status.SUCCESS)
        result_create1.exception should be(None)
        a[SQLException] should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_create").close()
        })
        val result_create2 = target.execute(execution, Phase.CREATE)
        result_create2.status should be(Status.SUCCESS)
        result_create2.exception should be(None)

        // ========== BUILD ===========================================================================================
        target.dirty(execution, Phase.BUILD) should be (Yes)
        val result_build1 = target.execute(execution, Phase.BUILD)
        result_build1.status should be(Status.SUCCESS)
        result_build1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_build").close()
        })
        val result_build2 = target.execute(execution, Phase.BUILD)
        result_build2.status should be(Status.FAILED)
        result_build2.exception.get shouldBe a[SQLException]

        // ========== VERIFY ==========================================================================================
        target.dirty(execution, Phase.VERIFY) should be (No)
        val result_verify1 = target.execute(execution, Phase.VERIFY)
        result_verify1.status should be(Status.SUCCESS)
        result_verify1.exception should be(None)
        a[SQLException] should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_verify").close()
        })
        val result_verify2 = target.execute(execution, Phase.VERIFY)
        result_verify2.status should be(Status.SUCCESS)
        result_verify2.exception should be(None)

        // ========== TRUNCATE ========================================================================================
        target.dirty(execution, Phase.TRUNCATE) should be(Unknown)
        val result_truncate1 = target.execute(execution, Phase.TRUNCATE)
        result_truncate1.status should be(Status.SUCCESS)
        result_truncate1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_truncate").close()
        })
        val result_truncate2 = target.execute(execution, Phase.TRUNCATE)
        result_truncate2.status should be(Status.FAILED)
        result_truncate2.exception.get shouldBe a[SQLException]

        // ========== DESTROY =========================================================================================
        target.dirty(execution, Phase.DESTROY) should be(Yes)
        val result_destroy1 = target.execute(execution, Phase.DESTROY)
        result_destroy1.status should be(Status.SUCCESS)
        result_destroy1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_destroy").close()
        })
        val result_destroy2 = target.execute(execution, Phase.DESTROY)
        result_destroy2.status should be(Status.FAILED)
        result_destroy2.exception.get shouldBe a[SQLException]

        session.shutdown()
    }

    it should "work in transactional mode" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:derby:" + db + ";create=true"
        val driver = "org.apache.derby.jdbc.EmbeddedDriver"
        val connection = JdbcConnection(
            Connection.Properties(context, "jdbc"),
            url,
            driver
        )
        val target = JdbcCommandTarget(
            Target.Properties(context, "jdbc"),
            connection = ConnectionReference(context, Prototype.of(connection.asInstanceOf[Connection])),
            validateAction = Some(Action(Seq("CREATE TABLE t_validate2(str_col CLOB)"), Some("VALUES(1)"), transactional=true)),
            createAction = Some(Action(Seq("CREATE TABLE t_create2(str_col CLOB)"), Some("VALUES(0)"), transactional=true)),
            buildAction = Some(Action(Seq("CREATE TABLE t_build2(str_col CLOB)"), None, transactional=true)),
            verifyAction = Some(Action(Seq("CREATE TABLE t_verify2(str_col CLOB)"), Some("SELECT 1 FROM (VALUES(1)) x WHERE 1 = 0"), transactional=true)),
            truncateAction = Some(Action(Seq("CREATE TABLE t_truncate2(str_col CLOB)"), Some("ILLEGAL SQL"), transactional=true)),
            destroyAction = Some(Action(Seq("CREATE TABLE t_destroy2(str_col CLOB)"), Some("VALUES(2)"), transactional=true))
        )

        // ========== VALIDATE ========================================================================================
        target.dirty(execution, Phase.VALIDATE) should be(Yes)
        val result_validate1 = target.execute(execution, Phase.VALIDATE)
        result_validate1.status should be(Status.SUCCESS)
        result_validate1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_validate").close()
        })
        val result_validate2 = target.execute(execution, Phase.VALIDATE)
        result_validate2.status should be(Status.FAILED)
        result_validate2.exception.get shouldBe a[SQLException]

        // ========== CREATE ==========================================================================================
        target.dirty(execution, Phase.CREATE) should be(No)
        val result_create1 = target.execute(execution, Phase.CREATE)
        result_create1.status should be(Status.SUCCESS)
        result_create1.exception should be(None)
        a[SQLException] should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_create").close()
        })
        val result_create2 = target.execute(execution, Phase.CREATE)
        result_create2.status should be(Status.SUCCESS)
        result_create2.exception should be(None)

        // ========== BUILD ===========================================================================================
        target.dirty(execution, Phase.BUILD) should be(Yes)
        val result_build1 = target.execute(execution, Phase.BUILD)
        result_build1.status should be(Status.SUCCESS)
        result_build1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_build").close()
        })
        val result_build2 = target.execute(execution, Phase.BUILD)
        result_build2.status should be(Status.FAILED)
        result_build2.exception.get shouldBe a[SQLException]

        // ========== VERIFY ==========================================================================================
        target.dirty(execution, Phase.VERIFY) should be(No)
        val result_verify1 = target.execute(execution, Phase.VERIFY)
        result_verify1.status should be(Status.SUCCESS)
        result_verify1.exception should be(None)
        a[SQLException] should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_verify").close()
        })
        val result_verify2 = target.execute(execution, Phase.VERIFY)
        result_verify2.status should be(Status.SUCCESS)
        result_verify2.exception should be(None)

        // ========== TRUNCATE ========================================================================================
        target.dirty(execution, Phase.TRUNCATE) should be(Unknown)
        val result_truncate1 = target.execute(execution, Phase.TRUNCATE)
        result_truncate1.status should be(Status.SUCCESS)
        result_truncate1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_truncate").close()
        })
        val result_truncate2 = target.execute(execution, Phase.TRUNCATE)
        result_truncate2.status should be(Status.FAILED)
        result_truncate2.exception.get shouldBe a[SQLException]

        // ========== DESTROY =========================================================================================
        target.dirty(execution, Phase.DESTROY) should be(Yes)
        val result_destroy1 = target.execute(execution, Phase.DESTROY)
        result_destroy1.status should be(Status.SUCCESS)
        result_destroy1.exception should be(None)
        noException should be thrownBy (withDatabase(driver, url) {
            _.executeQuery("SELECT COUNT(*) FROM t_destroy").close()
        })
        val result_destroy2 = target.execute(execution, Phase.DESTROY)
        result_destroy2.status should be(Status.FAILED)
        result_destroy2.exception.get shouldBe a[SQLException]

        session.shutdown()
    }
}
