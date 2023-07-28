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


class BinaryTypeTest extends AnyFlatSpec with Matchers {
    "A BinaryType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("binary") should be(BinaryType)
    }

    it should "parseable from a SQL string" in {
        FieldType.of("binary") should be(BinaryType)
        FieldType.of("BINARY") should be(BinaryType)
    }

    it should "not support parsing" in {
        an[NotImplementedError] should be thrownBy (BinaryType.parse(""))
        an[NotImplementedError] should be thrownBy (BinaryType.interpolate(SingleValue("")))
    }

    it should "provide the correct Spark type" in {
        BinaryType.sparkType should be (org.apache.spark.sql.types.BinaryType)
    }

    it should "provide the correct SQL type" in {
        BinaryType.sqlType should be ("BINARY")
        BinaryType.typeName should be ("binary")
        BinaryType.sparkType.sql should be ("BINARY")
    }
}
