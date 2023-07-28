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

import java.math.BigDecimal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class DecimalTypeTest extends AnyFlatSpec with Matchers {
    "A decimal type" should "be deserializable" in {
        val spec =
            """
              |decimal(10,4)
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result.asInstanceOf[DecimalType].precision should be (10)
        result.asInstanceOf[DecimalType].scale should be (4)
    }

    it should "parseable from a SQL string" in {
        FieldType.of("decimal(10,4)") should be(DecimalType(10,4))
    }

    it should "provide the correct Spark type" in {
        val ftype = DecimalType(10,4)
        ftype.sparkType should be (org.apache.spark.sql.types.DecimalType(10,4))
    }

    it should "provide the correct SQL type" in {
        val ftype = DecimalType(10,4)
        ftype.typeName should be ("decimal(10,4)")
        ftype.sqlType should be ("DECIMAL(10,4)")
        ftype.sparkType.sql should be ("DECIMAL(10,4)")
    }

    it should "support parsing" in {
        val ftype = DecimalType(10,4)
        ftype.parse("10.3") should be (new BigDecimal(103).divide(new BigDecimal(10)))
    }
}
