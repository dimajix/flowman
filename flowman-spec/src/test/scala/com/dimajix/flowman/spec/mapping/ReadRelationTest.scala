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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.SingleValue


class ReadRelationTest extends FlatSpec with Matchers {
    "A ReadRelationMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: readRelation
              |    relation: some_relation
              |    filter: "landing_date > 123"
              |    partitions:
              |      p0: "12"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t0"))

        mapping shouldBe a[ReadRelationMapping]
        val rrm = mapping.asInstanceOf[ReadRelationMapping]
        rrm.relation should be (RelationIdentifier("some_relation"))
        rrm.filter should be (Some("landing_date > 123"))
        rrm.partitions should be (Map("p0" -> SingleValue("12")))
    }
}
