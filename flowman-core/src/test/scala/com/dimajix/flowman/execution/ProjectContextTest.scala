package com.dimajix.flowman.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.Project


class ProjectContextTest extends FlatSpec with Matchers {
    "The ProjectContext" should "provide Project related vars" in {
        val spec =
            """
              |name: my_project
              |version: 1.0
            """.stripMargin
        val project = Project.read.string(spec)
        val session = Session.builder()
            .build()

        val context = session.createContext(project)
        context.evaluate("${Project.basedir}") should be ("")
        context.evaluate("${Project.filename}") should be ("")
        context.evaluate("${Project.name}") should be ("my_project")
        context.evaluate("${Project.version}") should be ("1.0")
    }
}
