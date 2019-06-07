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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class BooleanTypeTest extends FlatSpec with Matchers {
    "A BooleanType" should "parse strings" in {
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

    it should "provide the correct SQL type" in {
        val ftype = BooleanType

        ftype.sqlType should be ("boolean")
    }
}
