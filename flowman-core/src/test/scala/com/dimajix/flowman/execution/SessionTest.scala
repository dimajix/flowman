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
    }
}
