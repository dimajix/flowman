/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import com.google.common.io.Resources
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Project


class ProjectTest extends AnyFlatSpec with Matchers {
    "A Project" should "be parseable from a string" in {
        val spec =
            """
              |name: test
              |version: 1.0
              |
              |imports:
              |  - project: common
              |    job: some_job
              |    arguments:
              |      some_arg: $lala
            """.stripMargin
        val project = Project.read.string(spec)
        project.name should be ("test")
        project.version should be (Some("1.0"))
        project.filename should be (None)
        project.basedir should be (None)
        project.imports should be (Seq(
            Project.Import("common", job=Some("some_job"), arguments=Map("some_arg" -> "$lala"))
        ))
    }

    it should "be readable from a file" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val fs = FileSystem(new Configuration())
        val file = fs.file(new Path(basedir, "project/TestProject.yml"))
        val project = Project.read.file(file)
        project.name should be ("test")
        project.version should be (Some("1.0"))
        project.filename should be (Some(file.absolute))
        project.basedir should be (Some(file.absolute.parent))
        project.environment should contain("x" -> "y")
        project.config should contain("spark.lala" -> "lolo")
    }
}
