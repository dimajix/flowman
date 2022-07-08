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

package com.dimajix.flowman.spec.relation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spec.annotation.RelationType


@RelationType(kind = "annotatedRelation")
class AnnotationRelationSpec extends EmptyRelationSpec { }


class RelationSpecTest extends AnyFlatSpec with Matchers {
    "RelationSpec" should "support custom relations" in {
        val spec =
            """
              |relations:
              |  custom:
              |    kind: annotatedRelation
            """.stripMargin
        val module = Module.read.string(spec)
        module.relations.keys should contain("custom")
        module.relations("custom") shouldBe a[AnnotationRelationSpec]
    }
}
