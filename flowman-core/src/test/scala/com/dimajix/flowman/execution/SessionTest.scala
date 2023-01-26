/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.storage.AbstractStore
import com.dimajix.spark.testing.LocalSparkSession


class SessionTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "A Session" should "be buildable" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        session should not be (null)
        session.shutdown()
    }

    it should "contain a valid context" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        context should not be (null)
        context.execution should be (session.execution)

        session.shutdown()
    }

    it should "contain a valid runner" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        session.runner should not be (null)
        session.shutdown()
    }

    it should "create a valid Spark session" in {
        val session = Session.builder()
            .enableSpark()
            .build()
        session.sparkRunning should be (false)
        session.spark should not be (null)
        session.sparkRunning should be (true)
        session.shutdown()
    }

    it should "throw an exception when accessing Spark when it is disabled" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        session.sparkRunning should be (false)
        an[IllegalStateException] shouldBe thrownBy(session.spark)
        session.sparkRunning should be (false)
        session.shutdown()
    }

    it should "apply configs in correct order" in {
        val module = Module(
            environment = Map("x" -> "y"),
            config = Map("spark.lala" -> "lala.project", "spark.lili" -> "lili.project")
        )
        val project = module.toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig(Map("spark.lala" -> "lala_cmdline", "spark.lolo" -> "lolo_cmdline"))
            .withProject(project)
            .build()

        session.context.sparkConf.get("spark.lala") should be ("lala_cmdline")
        session.context.sparkConf.get("spark.lolo") should be ("lolo_cmdline")
        session.context.sparkConf.get("spark.lili") should be ("lili.project")
        session.sparkConf.get("spark.lala") should be ("lala_cmdline")
        session.sparkConf.get("spark.lolo") should be ("lolo_cmdline")
        session.sparkConf.get("spark.lili") should be ("lili.project")
        session.spark.conf.get("spark.lala") should be ("lala_cmdline")
        session.spark.conf.get("spark.lolo") should be ("lolo_cmdline")
        session.spark.conf.get("spark.lili") should be ("lili.project")
        session.spark.stop()
        session.shutdown()
    }

    it should "correctly propagate configurations" in {
        val module = Module(
            environment = Map("x" -> "y"),
            config = Map("spark.lala" -> "lala.project", "spark.lili" -> "lili.project")
        )
        val project = module.toProject("project")
        val session = Session.builder()
            .disableSpark()
            .withProject(project)
            .withConfig("spark.lala", "spark_lala")
            .withConfig("flowman.lolo", "flowman_lolo")
            .withConfig("other.abc", "other_abc")
            .build()

        session.context.sparkConf.get("spark.lili") should be ("lili.project")
        session.context.sparkConf.get("spark.lala") should be ("spark_lala")
        session.context.sparkConf.contains("flowman.lolo") should be (false)
        session.context.sparkConf.get("other.abc") should be ("other_abc")

        session.sparkConf.get("spark.lili") should be ("lili.project")
        session.sparkConf.get("spark.lala") should be ("spark_lala")
        session.sparkConf.contains("flowman.lolo") should be (false)
        session.sparkConf.get("other.abc") should be ("other_abc")

        session.flowmanConf.get("flowman.lolo") should be ("flowman_lolo")
        session.flowmanConf.contains("spark.lala") should be (false)
        session.flowmanConf.contains("other.abc") should be (false)

        session.config.get("spark.lala") should be ("spark_lala")
        session.config.get("flowman.lolo") should be ("flowman_lolo")
        session.config.get("other.abc") should be ("other_abc")
        session.shutdown()
    }

    it should "create new detached Sessions" in {
        val session = Session.builder()
            .enableSpark()
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
        session.execution should not equal(newSession.execution)
        session.runner should not equal(newSession.runner)

        session.shutdown()
        newSession.shutdown()
    }

    it should "set all Spark configs" in {
        val session = Session.builder()
            .enableSpark()
            .withConfig(Map("spark.lala" -> "lolo"))
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.lala") should be ("lolo")
        newSession.spark.conf.get("spark.lala") should be ("lolo")
        session.shutdown()
        newSession.shutdown()
    }

    it should "use the Spark application name from the session builder" in {
        val session = Session.builder()
            .enableSpark()
            .withSparkName("My Spark App")
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.app.name") should be ("My Spark App")
        newSession.spark.conf.get("spark.app.name") should be ("My Spark App")
        session.shutdown()
        newSession.shutdown()
    }

    it should "use the Spark application name from the configuration" in {
        val session = Session.builder()
            .enableSpark()
            .withConfig(Map("spark.app.name" -> "My Spark App"))
            .withSparkName("To be overriden")
            .build()

        val newSession = session.newSession()

        session.spark.conf.get("spark.app.name") should be ("My Spark App")
        newSession.spark.conf.get("spark.app.name") should be ("My Spark App")
        session.shutdown()
        newSession.shutdown()
    }

    it should "support importing projects via Stores" in {
        val project1JobGen = mock[Prototype[Job]]
        val project1 = Project(
            name = "project1",
            jobs = Map("main" -> project1JobGen),
            imports = Seq(
                Project.Import(project = "project2"),
                Project.Import(project = "project3")
            )
        )
        val project2 = Project(
            name = "project2",
            environment = Map("env1" -> "val1")
        )
        val project3JobGen = mock[Prototype[Job]]
        val project3 = Project(
            name = "project3",
            jobs = Map("daily" -> project3JobGen)
        )
        val project4 = Project(
            name = "project4"
        )
        val allProjects = Seq(project2, project3, project4)
        val store = new AbstractStore() {
            override def loadProject(name: String): Project = allProjects.find(_.name == name).get
            override def listProjects(): Seq[Project] = Seq()
        }

        val session = Session.builder()
            .withProject(project1)
            .withStore(store)
            .disableSpark()
            .build()
        val context = session.context

        a[NoSuchJobException] should be thrownBy(context.getJob(JobIdentifier("job")))

        a[NoSuchJobException] should be thrownBy(context.getJob(JobIdentifier("project1/lala")))

        val project1Job = Job.builder(context).build()
        (project1JobGen.instantiate _).expects(*,None).returns(project1Job)
        context.getJob(JobIdentifier("project1/main")) should be(project1Job)

        a[NoSuchJobException] should be thrownBy(context.getJob(JobIdentifier("project2/lala")))

        val project3Job = Job.builder(context).build()
        (project3JobGen.instantiate _).expects(*,None).returns(project3Job)
        context.getJob(JobIdentifier("project3/daily")) should be(project3Job)

        a[UnknownProjectException] should be thrownBy(context.getJob(JobIdentifier("project4/lala")))

        session.shutdown()
    }
}
