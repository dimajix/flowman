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

package com.dimajix.flowman.documentation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.NullType
import com.dimajix.flowman.types.StringType


class ColumnDocTest extends AnyFlatSpec with Matchers {
    "A ColumnDoc" should "support merge" in {
        val doc1 = ColumnDoc(
            None,
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some child desc 1")))
            )
        )
        val doc2 = ColumnDoc(
            None,
            Field("col2", DoubleType, description = Some("Some desc 2")),
            children = Seq(
                ColumnDoc(None, Field("child2", NullType, description = Some("Some override child desc 1"))),
                ColumnDoc(None, Field("child3", NullType, description = Some("Some override child desc 1")))
            )
        )

        val result = doc1.merge(doc2)

        result should be (ColumnDoc(
            None,
            Field("col1", DoubleType, description = Some("Some desc 2")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some override child desc 1"))),
                ColumnDoc(None, Field("child3", NullType, description = Some("Some override child desc 1")))
            )
        ))
    }

    it should "support reparent" in {
        val doc1 = ColumnDoc(
            None,
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some child desc 1")))
            )
        )
        val parent = SchemaDoc(None)

        val result = doc1.reparent(parent.reference)

        result should be (ColumnDoc(
            Some(parent.reference),
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(Some(ColumnReference(Some(parent.reference), "col1")), Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(Some(ColumnReference(Some(parent.reference), "col1")), Field("child2", StringType, description = Some("Some child desc 1")))
            )
        ))
    }
}
