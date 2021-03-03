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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module


class TemplateMappingTest extends AnyFlatSpec with Matchers {
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

        mapping.inputs should be (Seq(MappingOutputIdentifier("lala")))
    }
}
