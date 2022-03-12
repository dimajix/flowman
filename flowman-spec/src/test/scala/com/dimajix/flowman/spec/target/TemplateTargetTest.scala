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

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.TargetIdentifier


class TemplateTargetTest extends AnyFlatSpec with Matchers {
    "A TemplateTarget" should "work" in {
        val spec =
            """
              |targets:
              |  xfs:
              |    kind: null
              |
              |  template:
              |    kind: template
              |    target: xfs
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("template"))
        target should not be (null)
        target shouldBe a[TemplateTarget]
        target.name should be ("template")
    }

    it should "provide own documentation" in {
        val spec =
            """
              |targets:
              |  xfs:
              |    kind: null
              |
              |  template:
              |    kind: template
              |    target: xfs
              |    documentation:
              |      description: "This is the template"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("template"))
        val doc = target.documentation.get
        doc.target should be (Some(target))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
    }

    it should "provide templated documentation" in {
        val spec =
            """
              |targets:
              |  xfs:
              |    kind: null
              |    documentation:
              |      description: "This is the original target"
              |
              |  template:
              |    kind: template
              |    target: xfs
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("template"))
        val doc = target.documentation.get
        doc.target should be (Some(target))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the original target"))
    }

    it should "provide merged documentation" in {
        val spec =
            """
              |targets:
              |  xfs:
              |    kind: null
              |    documentation:
              |      description: "This is the original target"
              |
              |  template:
              |    kind: template
              |    target: xfs
              |    documentation:
              |      description: "This is the template"
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("template"))
        val doc = target.documentation.get
        doc.target should be (Some(target))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
    }
}
