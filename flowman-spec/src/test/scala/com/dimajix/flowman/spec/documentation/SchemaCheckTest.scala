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

package com.dimajix.flowman.spec.documentation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.documentation.ForeignKeySchemaCheck
import com.dimajix.flowman.documentation.PrimaryKeySchemaCheck
import com.dimajix.flowman.documentation.SchemaReference
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper


class SchemaCheckTest extends AnyFlatSpec with Matchers {
    "A PrimaryKeySchemaCheck" should "be deserializable" in {
        val yaml =
            """
              |kind: primaryKey
              |columns: [a,b]
              |filter: "x IS NOT NULL"
            """.stripMargin

        val spec = ObjectMapper.parse[SchemaCheckSpec](yaml)
        spec shouldBe a[PrimaryKeySchemaCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, SchemaReference(None))
        test should be (PrimaryKeySchemaCheck(
            Some(SchemaReference(None)),
            columns = Seq("a","b"),
            filter = Some("x IS NOT NULL")
        ))
    }

    "A ForeignKeySchemaCheckSpec" should "be deserializable" in {
        val yaml =
            """
              |kind: foreignKey
              |mapping: reference
              |columns: [a,b]
              |references: [c,d]
              |filter: "x IS NOT NULL"
            """.stripMargin

        val spec = ObjectMapper.parse[SchemaCheckSpec](yaml)
        spec shouldBe a[ForeignKeySchemaCheckSpec]

        val context = RootContext.builder().build()
        val test = spec.instantiate(context, SchemaReference(None))
        test should be (ForeignKeySchemaCheck(
            Some(SchemaReference(None)),
            columns = Seq("a","b"),
            mapping = Some(MappingOutputIdentifier("reference")),
            references = Seq("c","d"),
            filter = Some("x IS NOT NULL")
        ))
    }
}
