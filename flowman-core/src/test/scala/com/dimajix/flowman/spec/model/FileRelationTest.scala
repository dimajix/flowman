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

package com.dimajix.flowman.spec.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.Module


class FileRelationTest extends FlatSpec with Matchers {
    "The FileRelation" should "be parseable" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: file
              |    format: sequencefile
              |    location: "lala.seq"
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("t0")
    }
}
