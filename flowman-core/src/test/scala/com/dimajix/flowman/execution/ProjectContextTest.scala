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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.model.Project


class ProjectContextTest extends AnyFlatSpec with Matchers {
    "The ProjectContext" should "provide Project related vars" in {
        val project = Project(
            name = "my_project",
            version = Some("1.0")
        )
        val session = Session.builder()
            .disableSpark()
            .build()

        val context = session.getContext(project)
        context.evaluate("${project.basedir}") should be ("")
        context.evaluate("${project.filename}") should be ("")
        context.evaluate("${project.name}") should be ("my_project")
        context.evaluate("${project.version}") should be ("1.0")

        session.shutdown()
    }

    it should "correctly interpolate project variables" in {
        val fs = FileSystem(new Configuration())
        val file = fs.file("test/project/TestProject.yml")
        val project = Project(
            name = "test",
            version = Some("1.0"),
            filename = Some(file.absolute),
            basedir = Some(file.absolute.parent)
        )
        val session = Session.builder()
            .disableSpark()
            .build()

        val context = session.getContext(project)
        context.evaluate("${project.name}") should be ("test")
        context.evaluate("${project.version}") should be ("1.0")
        context.evaluate("${project.basedir}") should be (file.absolute.parent.toString)
        context.evaluate("${project.basedir.parent}") should be (file.absolute.parent.parent.toString)
        context.evaluate("${project.filename}") should be (file.absolute.toString)
        context.evaluate("${project.filename.withSuffix('lala')}") should be (file.absolute.withSuffix("lala").toString)

        session.shutdown()
    }
}
