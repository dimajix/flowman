package com.dimajix.flowman.spec

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ProjectTest extends FlatSpec with Matchers {
    "A Project" should "be parseable from a string" in {
        val spec =
            """
              |name: test
              |version: 1.0
            """.stripMargin
        val project = Project.read.string(spec)
        project.name should be ("test")
        project.version should be ("1.0")
        project.filename.toString should be ("")
        project.basedir.toString should be ("")
    }

    it should "be readable from a file" in {
        val file = new File("test/project/TestProject.yml")
        val project = Project.read.file(file)
        project.name should be ("test")
        project.version should be ("1.0")
        project.filename.toString should be (file.getAbsoluteFile.toString)
        project.basedir.toString should be (file.getAbsoluteFile.getParent.toString)
        project.environment should contain("x" -> "y")
        project.config should contain("spark.lala" -> "lolo")
    }

    it should "contain a default main Job" in {
        val spec =
            """
              |name: test
              |version: 1.0
            """.stripMargin
        val project = Project.read.string(spec)
        project.main should be (Seq("main"))
    }

    it should "support an explicit default main Job" in {
        val spec =
            """
              |name: test
              |version: 1.0
              |main: lala
            """.stripMargin
        val project = Project.read.string(spec)
        project.main should be (Seq("lala"))
    }

    it should "support a list of explicit default main Job" in {
        val spec =
            """
              |name: test
              |version: 1.0
              |main:
              |  - lala
              |  - lolo
            """.stripMargin
        val project = Project.read.string(spec)
        project.main should be (Seq("lala", "lolo"))
    }
}
