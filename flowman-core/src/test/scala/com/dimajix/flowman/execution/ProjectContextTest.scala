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

import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.fs.FileSystem
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

        val context = session.getContext(project)
        context.evaluate("${project.basedir}") should be ("")
        context.evaluate("${project.filename}") should be ("")
        context.evaluate("${project.name}") should be ("my_project")
        context.evaluate("${project.version}") should be ("1.0")
    }

    it should "correctly resolve project variables" in {
        val fs = FileSystem(new Configuration())
        val file = fs.file("test/project/TestProject.yml")
        val project = Project.read.file(file)
        val session = Session.builder()
            .build()

        val context = session.getContext(project)
        context.evaluate("${project.name}") should be ("test")
        context.evaluate("${project.version}") should be ("1.0")
        context.evaluate("${project.basedir}") should be (file.absolute.parent.toString)
        context.evaluate("${project.basedir.parent}") should be (file.absolute.parent.parent.toString)
        context.evaluate("${project.filename}") should be (file.absolute.toString)
        context.evaluate("${project.filename.withSuffix('lala')}") should be (file.absolute.withSuffix("lala").toString)
    }
}
