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

import com.dimajix.flowman.spec.Module

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

    it should "apply configs in correct order" in {
        val spec =
            """
              |environment:
              |  - x=y
              |config:
              |  - spark.lala=lala_project
              |  - spark.lili=lili_project
              """.stripMargin
        val module = Module.read.string(spec)
        val project = module.toProject("project")
        val session = Session.builder()
            .withSparkConfig(Map("spark.lala" -> "lala_cmdline", "spark.lolo" -> "lolo_cmdline"))
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

        val newSession = session.newSession(null)
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
}
