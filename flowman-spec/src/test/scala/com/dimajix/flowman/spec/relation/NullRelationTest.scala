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

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.common.No
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Relation
import com.dimajix.spark.testing.LocalSparkSession


class NullRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The NullRelation" should "support the full lifecycle" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val relation = NullRelation(Relation.Properties(session.context))
        val schema = StructType(
            StructField("lala", StringType) :: Nil
        )

        // == Create ===================================================================
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Unknown)
        relation.create(executor)

        // == Read ===================================================================
        val df = relation.read(executor, Some(schema))
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
