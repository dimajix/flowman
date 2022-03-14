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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spec.annotation.RelationType


@RelationType(kind = "annotatedMapping")
class AnnotationMappingSpec extends MappingSpec {
    override def instantiate(context: Context, properties:Option[Mapping.Properties]): Mapping = ???
}

class MappingSpecTest extends AnyFlatSpec with Matchers {
    "MappingSpec" should "support custom mappings" in {
        val spec =
            """
              |mappings:
              |  custom:
              |    kind: annotatedMapping
            """.stripMargin
        val module = Module.read.string(spec)
        module.mappings.keys should contain("custom")
        module.mappings("custom") shouldBe a[AnnotationMappingSpec]
    }
}
