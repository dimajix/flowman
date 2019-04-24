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

package com.dimajix.flowman.transforms.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType


class SchemaTreeTest extends FlatSpec with Matchers {
    "The SchemaTree" should "create the same schema via round-trip" in {
        import com.dimajix.flowman.transforms.schema.SchemaTree.implicits._

        val inputSchema = StructType(Seq(
            Field("col1", StringType),
            Field("COL2", StructType(
                Seq(
                    Field("nested1", StringType),
                    Field("nested3", ArrayType(FloatType)),
                    Field("nested4", StructType(
                        Seq(
                            Field("nested4_1", StringType),
                            Field("nested4_2", FloatType)
                        )
                    ))
                )
            )),
            Field("col3", ArrayType(StructType(
                Seq(
                    Field("nested1", StringType),
                    Field("nested3", IntegerType)
                )
            )))
        ))
        val root = SchemaTree.ofSchema(inputSchema)
        val columns = root.mkValue()

        columns.ftype should be (inputSchema)
    }
}
