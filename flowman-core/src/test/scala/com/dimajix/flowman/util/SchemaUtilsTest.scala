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

package com.dimajix.flowman.util

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class SchemaUtilsTest extends FlatSpec with Matchers {
    "SchemaUtils" should "convert all names to lower case" in {
        val schema = StructType(
            StructField("Name", StringType) ::
            StructField("Nested", StructType(
                StructField("AmOUnt", DoubleType) ::
                StructField("SomeArray", ArrayType(IntegerType)) ::
                Nil
            )) ::
                StructField("StructArray", ArrayType(
                    StructType(
                        StructField("Name", StringType) ::
                        Nil
                    )
                )
            ) ::
            Nil
        )

        val expected = StructType(
            StructField("name", StringType) ::
                StructField("nested", StructType(
                    StructField("amount", DoubleType) ::
                        StructField("somearray", ArrayType(IntegerType)) ::
                        Nil
                )) ::
                StructField("structarray", ArrayType(
                    StructType(
                        StructField("name", StringType) ::
                            Nil
                    )
                )
                ) ::
                Nil
        )

        val result = SchemaUtils.toLowerCase(schema)
        result should be(expected)
    }
}
