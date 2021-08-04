/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class CaseMappingTest extends AnyFlatSpec with Matchers {
    val spec =
        """
          |mappings:
          |  switch:
          |    kind: case
          |    cases:
          |      - condition: $env == '1'
          |        input: in_1
          |      - condition: $env == '2'
          |        input: in_2
          |      - condition: true
          |        input: default
          |""".stripMargin

    "A CaseMapping" should "select the default case" in {
        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("switch")

        mapping shouldBe an[CaseMappingSpec]

        val session = Session.builder().disableSpark().withEnvironment("env", "").build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("switch")).asInstanceOf[AliasMapping]
        instance shouldBe an[AliasMapping]
        instance.input should be (MappingOutputIdentifier("default"))
    }

    it should "select the first valid case" in {
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().disableSpark().withEnvironment("env", "2").build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("switch")).asInstanceOf[AliasMapping]
        instance shouldBe an[AliasMapping]
        instance.input should be (MappingOutputIdentifier("in_2"))

    }
}
