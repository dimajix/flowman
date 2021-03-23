/*
 * Copyright 2021 Kaya Kupferschmidt
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


class NullTypeTest extends AnyFlatSpec with Matchers {
    "A NullType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("\"null\"") should be(NullType)
    }

    it should "not support parsing" in {
        an[NotImplementedError] should be thrownBy (NullType.parse(""))
        an[NotImplementedError] should be thrownBy (NullType.interpolate(SingleValue("")))
    }

    it should "provide the correct Spark type" in {
        NullType.sparkType should be (org.apache.spark.sql.types.NullType)
    }

    it should "provide the correct SQL type" in {
        NullType.sqlType should be ("null")
        NullType.sparkType.sql should be ("NULL")
        NullType.typeName should be ("null")
    }
}
