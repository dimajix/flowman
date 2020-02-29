/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Project


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
        val fs = FileSystem(new Configuration())
        val file = fs.file("test/project/TestProject.yml")
        val project = Project.read.file(file)
        project.name should be ("test")
        project.version should be ("1.0")
        project.filename.toString should be (file.absolute.toString)
        project.basedir.toString should be (file.absolute.parent.toString)
        project.environment should contain("x" -> "y")
        project.config should contain("spark.lala" -> "lolo")
    }
}
