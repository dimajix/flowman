/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Module


class DocumentTargetTest extends AnyFlatSpec with Matchers {
    "A DocumentTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  docu:
              |    kind: document
              |    collectors:
              |      # Collect documentation of relations
              |      - kind: relations
              |      # Collect documentation of mappings
              |      - kind: mappings
              |      # Collect documentation of build targets
              |      - kind: targets
              |      # Execute all checks
              |      - kind: checks
              |
              |    generators:
              |      # Create an output file in the project directory
              |      - kind: file
              |        location: ${project.basedir}/generated-documentation
              |        template: html
              |        excludeRelations:
              |          # You can either specify a name (without the project)
              |          - "stations_raw"
              |          # Or can also explicitly specify a name with the project
              |          - ".*/measurements_raw"
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("docu")
        target shouldBe an[DocumentTargetSpec]
    }

}
