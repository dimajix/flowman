/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module


class TemplateMappingTest extends FlatSpec with Matchers {
    "A TemplateMapping" should "work" in {
        val spec =
            """
              |mappings:
              |  xfs:
              |    kind: schema
              |    input: ${input}
              |    columns:
              |      _2: string
              |      _1: string
              |
              |  template:
              |    kind: template
              |    mapping: xfs
              |    environment:
              |      - input=$default_input
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .withEnvironment("default_input", "lala")
            .build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("template"))
        mapping should not be (null)
        mapping shouldBe a[TemplateMapping]

        mapping.dependencies should be (Seq(MappingOutputIdentifier("lala")))
    }
}
