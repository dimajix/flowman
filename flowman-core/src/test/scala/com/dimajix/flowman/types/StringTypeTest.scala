/*
 * Copyright (C) 2018 The Flowman Authors
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


class StringTypeTest extends AnyFlatSpec with Matchers {
    "A StringType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("string") should be(StringType)
    }

    it should "parse strings" in {
        StringType.parse("lala") should be ("lala")
    }

    it should "support interpolation of SingleValues" in {
        StringType.interpolate(SingleValue("lala"), null).head should be ("lala")
    }

    it should "support interpolation of ArrayValues" in {
        val result = StringType.interpolate(ArrayValue(Array("12","27")), null).toSeq
        result(0) should be ("12")
        result(1) should be ("27")
    }

    it should "provide the correct Spark type" in {
        StringType.sparkType should be (org.apache.spark.sql.types.StringType)
    }

    it should "provide the correct SQL type" in {
        StringType.sqlType should be ("string")
        StringType.sparkType.sql should be ("STRING")
    }

}
