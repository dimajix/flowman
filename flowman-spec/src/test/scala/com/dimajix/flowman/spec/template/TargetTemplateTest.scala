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
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.target.BlackholeTarget
import com.dimajix.flowman.spec.target.TargetSpec


class TargetTemplateTest extends AnyFlatSpec with Matchers {
    "A TargetTemplateInstance" should "be deserialized" in {
        val spec =
            """
              |kind: template/user
              |arg1: value_1
              |arg2: value_2
              |""".stripMargin

        val target = ObjectMapper.parse[TargetSpec](spec)
        target shouldBe a[TargetTemplateInstanceSpec]

        val targetTemplate = target.asInstanceOf[TargetTemplateInstanceSpec]
        targetTemplate.args should be (Map("arg1" -> "value_1", "arg2" -> "value_2"))
    }

    it should "work" in {
        val spec =
            """
              |templates:
              |  user:
              |    kind: target
              |    parameters:
              |      - name: p0
              |        type: string
              |      - name: p1
              |        type: int
              |        default: 12
              |    template:
              |      kind: blackhole
              |      mapping: $p0
              |
              |targets:
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

        val tgt_1 = context.getTarget(TargetIdentifier("rel_1"))
        tgt_1 shouldBe a[TargetTemplateInstance]
        tgt_1.asInstanceOf[TargetTemplateInstance].instance shouldBe a[BlackholeTarget]
        tgt_1.name should be ("rel_1")
        tgt_1.identifier should be (TargetIdentifier("project/rel_1"))
        tgt_1.kind should be ("blackhole")

        an[IllegalArgumentException] should be thrownBy(context.getTarget(TargetIdentifier("rel_2")))

        val tgt_3 = context.getTarget(TargetIdentifier("rel_3"))
        tgt_3 shouldBe a[TargetTemplateInstance]
        tgt_3.asInstanceOf[TargetTemplateInstance].instance shouldBe a[BlackholeTarget]
        tgt_3.name should be ("rel_3")
        tgt_3.identifier should be (TargetIdentifier("project/rel_3"))
        tgt_3.kind should be ("blackhole")

        an[IllegalArgumentException] should be thrownBy(context.getTarget(TargetIdentifier("rel_4")))
    }

    it should "throw an error on unknown templates" in {
        val spec =
            """
              |targets:
              |  rel_1:
              |    kind: template/user
              |    p0: some_value
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        an[NoSuchTemplateException] should be thrownBy(context.getTarget(TargetIdentifier("rel_1")))
    }

    it should "forward before and after" in {
        val spec =
            """
              |templates:
              |  user:
              |    kind: target
              |    parameters:
              |      - name: p0
              |        type: string
              |      - name: p1
              |        type: int
              |        default: 12
              |    template:
              |      kind: blackhole
              |      mapping: $p0
              |      before: x
              |      after: y
              |
              |targets:
              |  rel_1:
              |    kind: template/user
              |    before: a
              |    after:
              |     - c
              |     - d
              |    p0: some_value
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val tgt = context.getTarget(TargetIdentifier("rel_1"))
        tgt shouldBe a[TargetTemplateInstance]
        tgt.asInstanceOf[TargetTemplateInstance].instance shouldBe a[BlackholeTarget]
        tgt.name should be ("rel_1")
        tgt.before should be (Seq(TargetIdentifier("a"), TargetIdentifier("x")))
        tgt.after should be (Seq(TargetIdentifier("c"), TargetIdentifier("d"), TargetIdentifier("y")))
    }
}
