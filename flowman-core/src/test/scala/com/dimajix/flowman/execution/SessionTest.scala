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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.model.Module


class SessionTest extends FlatSpec with Matchers {
    "A Session" should "be buildable" in {
        val session = Session.builder()
            .build()
        session should not be (null)
    }

    it should "contain a valid context" in {
        val session = Session.builder()
            .build()
        session.context should not be (null)
    }

    it should "contain a valid runner" in {
        val session = Session.builder()
            .build()
        session.runner should not be (null)
    }

    it should "create a valid Spark session" in {
        val session = Session.builder()
            .build()
        session.sparkRunning should be (false)
        session.spark should not be (null)
        session.sparkRunning should be (true)
    }

    it should "throw an exception when accessing Spark when it is disabled" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        session.sparkRunning should be (false)
        an[IllegalStateException] shouldBe thrownBy(session.spark)
        session.sparkRunning should be (false)
    }

    it should "apply configs in correct order" in {
        val module = Module(
            environment = Map("x" -> "y"),
            config = Map("spark.lala" -> "lala.project", "spark.lili" -> "lili.project")
        )
        val project = module.toProject("project")
        val session = Session.builder()
            .withConfig(Map("spark.lala" -> "lala_cmdline", "spark.lolo" -> "lolo_cmdline"))
            .withProject(project)
            .build()
        session.spark.conf.get("spark.lala") should be ("lala_cmdline")
        session.spark.conf.get("spark.lolo") should be ("lolo_cmdline")
        session.spark.conf.get("spark.lili") should be ("lili_project")
        session.spark.stop()
    }

    it should "create new detached Sessions" in {
        val session = Session.builder()
            .build()
        session.sparkRunning should be (false)

        val newSession = session.newSession()
        session.sparkRunning should be (false)
        newSession.sparkRunning should be (false)

        newSession.spark should not be (null)
        session.sparkRunning should be (true)
        newSession.sparkRunning should be (true)

        session.spark should not equal(newSession.spark)
        session.context should not equal(newSession.context)
        session.executor should not equal(newSession.executor)
        session.runner should not equal(newSession.runner)
    }

    it should "set all Spark configs" in {
        val session = Session.builder()
            .withConfig(Map("spark.lala" -> "lolo"))
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.lala") should be ("lolo")
        newSession.spark.conf.get("spark.lala") should be ("lolo")
    }

    it should "use the Spark application name from the session builder" in {
        val session = Session.builder()
            .withSparkName("My Spark App")
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.app.name") should be ("My Spark App")
        newSession.spark.conf.get("spark.app.name") should be ("My Spark App")
    }

    it should "use the Spark application name from the configuration" in {
        val session = Session.builder()
            .withConfig(Map("spark.app.name" -> "My Spark App"))
            .withSparkName("To be overriden")
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.app.name") should be ("My Spark App")
        newSession.spark.conf.get("spark.app.name") should be ("My Spark App")
    }
}
