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


class BooleanTypeTest extends AnyFlatSpec with Matchers {
    "A BooleanType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("boolean") should be(BooleanType)
    }

    it should "parseable from a SQL string" in {
        FieldType.of("bool") should be (BooleanType)
        FieldType.of("boolean") should be (BooleanType)
    }

    it should "parse strings" in {
        BooleanType.parse("true") should be (true)
        BooleanType.parse("false") should be (false)
    }

    it should "support interpolation of SingleValues" in {
        BooleanType.interpolate(SingleValue("true")).head should be (true)
    }

    it should "support interpolation of ArrayValues" in {
        val result = BooleanType.interpolate(ArrayValue(Array("true","false")))
        result.head should be (true)
        result.drop(1).head should be (false)
    }

    it should "provide the correct Spark type" in {
        BooleanType.sparkType should be (org.apache.spark.sql.types.BooleanType)
    }

    it should "provide the correct SQL type" in {
        BooleanType.typeName should be ("boolean")
        BooleanType.sqlType should be ("BOOLEAN")
        BooleanType.sparkType.sql should be ("BOOLEAN")
    }
}
