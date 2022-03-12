/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.template

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.NoSuchTemplateException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.relation.ValuesRelation


class RelationTemplateTest extends AnyFlatSpec with Matchers {
    "A RelationTemplateInstance" should "be deserialized" in {
        val spec =
            """
              |kind: template/user
              |arg1: value_1
              |arg2: value_2
              |""".stripMargin

        val relation = ObjectMapper.parse[RelationSpec](spec)
        relation shouldBe a[RelationTemplateInstanceSpec]

        val relationTemplate = relation.asInstanceOf[RelationTemplateInstanceSpec]
        relationTemplate.args should be (Map("arg1" -> "value_1", "arg2" -> "value_2"))
    }

    it should "work" in {
        val spec =
            """
              |templates:
              |  user:
              |    kind: relation
              |    parameters:
              |      - name: p0
              |        type: string
              |      - name: p1
              |        type: int
              |        default: 12
              |    template:
              |      kind: values
              |      records:
              |        - ["$p0",$p1]
              |      schema:
              |        kind: embedded
              |        fields:
              |          - name: str_col
              |            type: string
              |          - name: int_col
              |            type: integer
              |
              |relations:
              |  rel_1:
              |    kind: template/user
              |    p0: some_value
              |  rel_2:
              |    kind: template/user
              |    p1: 13
              |  rel_3:
              |    kind: template/user
              |    p0: some_value
              |    p1: 27
              |  rel_4:
              |    kind: template/user
              |    p0: some_value
              |    p3: no_such_param
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val rel_1 = context.getRelation(RelationIdentifier("rel_1"))
        rel_1 shouldBe a[RelationTemplateInstance]
        rel_1.asInstanceOf[RelationTemplateInstance].instance shouldBe a[ValuesRelation]
        rel_1.name should be ("rel_1")
        rel_1.identifier should be (RelationIdentifier("project/rel_1"))
        rel_1.kind should be ("values")

        an[IllegalArgumentException] should be thrownBy(context.getRelation(RelationIdentifier("rel_2")))

        val rel_3 = context.getRelation(RelationIdentifier("rel_3"))
        rel_3 shouldBe a[RelationTemplateInstance]
        rel_3.asInstanceOf[RelationTemplateInstance].instance shouldBe a[ValuesRelation]
        rel_3.name should be ("rel_3")
        rel_3.identifier should be (RelationIdentifier("project/rel_3"))
        rel_3.kind should be ("values")

        an[IllegalArgumentException] should be thrownBy(context.getRelation(RelationIdentifier("rel_4")))
    }

    it should "throw an error on unknown templates" in {
        val spec =
            """
              |relations:
              |  rel_1:
              |    kind: template/user
              |    p0: some_value
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        an[NoSuchTemplateException] should be thrownBy(context.getRelation(RelationIdentifier("rel_1")))
    }
}
