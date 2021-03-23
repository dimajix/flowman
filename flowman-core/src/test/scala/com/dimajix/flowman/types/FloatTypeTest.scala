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

package com.dimajix.flowman.types

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class FloatTypeTest extends AnyFlatSpec with Matchers {
    "A FloatType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("float") should be(FloatType)
    }

    it should "parse strings" in {
        FloatType.parse("1.0") should be (1.0)
    }

    it should "support interpolation of SingleValues" in {
        FloatType.interpolate(SingleValue("1.0")).head should be (1.0)
    }

    it should "support interpolation of ArrayValues" in {
        val result = FloatType.interpolate(ArrayValue("1.0", "2.0")).toSeq
        result(0) should be (1.0)
        result(1) should be (2.0)
    }

    it should "provide the correct Spark type" in {
        FloatType.sparkType should be (org.apache.spark.sql.types.FloatType)
    }

    it should "provide the correct SQL type" in {
        FloatType.sqlType should be ("float")
        FloatType.sparkType.sql should be ("FLOAT")
        FloatType.typeName should be ("float")
    }
}
