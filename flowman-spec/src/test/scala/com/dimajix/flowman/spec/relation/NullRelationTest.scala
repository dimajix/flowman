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

import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


class NullRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The NullRelation" should "be parsable" in {
        val spec =
            """
              |kind: null
              |name: ${rel_name}
              |""".stripMargin

        val relation = ObjectMapper.parse[RelationSpec](spec)
        relation shouldBe a[NullRelationSpec]

        val session = Session.builder().withEnvironment("rel_name", "abc").disableSpark().build()
        val context = session.context

        val instance = relation.instantiate(context)
        instance shouldBe a[NullRelation]
        instance.name should be ("abc")
        instance.identifier should be (TargetIdentifier("abc"))
    }

    it should "support the full lifecycle" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val relation = NullRelation(
            Relation.Properties(session.context),
            schema = Some(InlineSchema(
                Schema.Properties(session.context),
                fields = Seq(
                    Field("lala", StringType)
                )
            ))
        )

        // == Create ===================================================================
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Unknown)
        relation.create(executor)

        // == Read ===================================================================
        val df = relation.read(executor)
        df.count() should be (0)

        // == Truncate ===================================================================
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Unknown)

        // == Destroy ===================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Unknown)
    }
}
